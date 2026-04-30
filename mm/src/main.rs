use std::{env, sync::Arc};

use anyhow::{Context, Result};
use market_making::{
    adapters::exchange::{
        extended::ExtendedClient, grvt::GrvtClient, hibachi::HibachiClient, AnyExchangeClient,
    },
    adapters::notifier::telegram::{TelegramCommand, TelegramNotifier},
    adapters::storage::sqlite::FillStore,
    config::{AppConfig, ExchangeKind},
    core::{
        cleanup::flatten_account_state, engine::MarketMakingEngine, runtime_control::RuntimeControl,
    },
};
use tokio::time::{sleep, Duration};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,market_making=debug")),
        )
        .init();

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config/settings.toml".to_string());
    let config = AppConfig::from_path(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;
    let cleanup_min_notional = config.parsed()?.model.min_trade_amount;

    let notifier = TelegramNotifier::from_config(config.telegram.clone());
    let exchange = Arc::new(match config.venue.kind {
        ExchangeKind::Hibachi => {
            AnyExchangeClient::Hibachi(HibachiClient::new(config.venue.clone(), &config.network)?)
        }
        ExchangeKind::Grvt => {
            AnyExchangeClient::Grvt(GrvtClient::new(config.venue.clone(), &config.network)?)
        }
        ExchangeKind::Extended => {
            AnyExchangeClient::Extended(ExtendedClient::new(config.venue.clone(), &config.network)?)
        }
    });
    let fill_store = Arc::new(FillStore::from_config(config.storage.as_ref())?);
    let control = Arc::new(RuntimeControl::default());
    let engine = MarketMakingEngine::new(
        config.clone(),
        exchange.clone(),
        notifier.clone(),
        fill_store,
        control.clone(),
    );
    let telegram_task = tokio::spawn(run_telegram_command_loop(
        config.clone(),
        exchange.clone(),
        notifier.clone(),
        control,
    ));

    if config.runtime.dry_run {
        info!("dry-run enabled; skipping startup cancel-all");
    } else {
        flatten_account_state(
            exchange.as_ref(),
            &notifier,
            "startup",
            cleanup_min_notional,
        )
        .await?;
    }

    let result = tokio::select! {
        result = engine.run() => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed waiting for ctrl-c")?;
            Ok(())
        }
    };

    if config.runtime.dry_run {
        info!("dry-run enabled; skipping shutdown cancel-all");
    } else {
        if let Err(err) = flatten_account_state(
            exchange.as_ref(),
            &notifier,
            "shutdown",
            cleanup_min_notional,
        )
        .await
        {
            error!(?err, "shutdown account cleanup failed");
            notifier
                .send(format!("market-making shutdown cleanup failed: {err:#}"))
                .await;
        }
    }

    match result {
        Ok(()) => {
            telegram_task.abort();
            Ok(())
        }
        Err(err) => {
            telegram_task.abort();
            notifier
                .send(format!("market-making stopped with error: {err:#}"))
                .await;
            Err(err)
        }
    }
}

async fn run_telegram_command_loop(
    config: AppConfig,
    exchange: Arc<AnyExchangeClient>,
    notifier: TelegramNotifier,
    control: Arc<RuntimeControl>,
) -> Result<()> {
    let cleanup_min_notional = config.parsed()?.model.min_trade_amount;
    let mut next_offset = None;
    let mut ignore_first_batch = true;
    let telegram_enabled = config
        .telegram
        .as_ref()
        .map(|cfg| cfg.enabled)
        .unwrap_or(false);

    loop {
        if !telegram_enabled {
            sleep(Duration::from_secs(60)).await;
            continue;
        }

        match notifier.poll_commands(next_offset, 15).await {
            Ok(commands) => {
                if let Some(max_update_id) = commands.iter().map(|(id, _)| *id).max() {
                    next_offset = Some(max_update_id + 1);
                }
                if ignore_first_batch {
                    ignore_first_batch = false;
                    continue;
                }

                for (_, command) in commands {
                    match command {
                        TelegramCommand::Start => {
                            control.resume();
                            notifier.send("bot resumed").await;
                        }
                        TelegramCommand::Stop => {
                            control.pause();
                            if config.runtime.dry_run {
                                notifier.send("bot paused in dry run").await;
                            } else {
                                match flatten_account_state(
                                    exchange.as_ref(),
                                    &notifier,
                                    "telegram-stop",
                                    cleanup_min_notional,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        notifier
                                            .send("bot stopped; orders canceled and limit-close cleanup attempted")
                                            .await
                                    }
                                    Err(err) => {
                                        notifier.send(format!("bot stop failed: {err:#}")).await
                                    }
                                }
                            }
                        }
                        TelegramCommand::Restart => {
                            control.pause();
                            if config.runtime.dry_run {
                                control.resume();
                                notifier.send("bot restarted in dry run").await;
                            } else {
                                match flatten_account_state(
                                    exchange.as_ref(),
                                    &notifier,
                                    "telegram-restart",
                                    cleanup_min_notional,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        control.resume();
                                        notifier
                                            .send("bot restarted; limit-close cleanup attempted before resume")
                                            .await;
                                    }
                                    Err(err) => {
                                        notifier.send(format!("bot restart failed: {err:#}")).await
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!(?err, "telegram command polling failed");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}
