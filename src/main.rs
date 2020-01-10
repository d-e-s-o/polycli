// Copyright (C) 2020 Daniel Mueller <deso@posteo.net>
// SPDX-License-Identifier: GPL-3.0-or-later

use std::io::stdout;
use std::io::Write;
use std::process::exit;

use anyhow::Context;
use anyhow::Error;

use polyio::api::ticker;
use polyio::Client;

use structopt::StructOpt;

use tokio::runtime::Runtime;

use tracing::subscriber::set_global_default as set_global_subscriber;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::FmtSubscriber;


/// A command line client for interacting with the Polygon.io API.
#[derive(Debug, StructOpt)]
struct Opts {
  #[structopt(subcommand)]
  command: Command,
  /// Increase verbosity (can be supplied multiple times).
  #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
  verbosity: usize,
}

/// A command line client for automated trading with Alpaca.
#[derive(Debug, StructOpt)]
enum Command {
  /// Retrieve tickers.
  #[structopt(name = "ticker")]
  Ticker(Ticker),
}


/// An enumeration representing the `ticker` command.
#[derive(Debug, StructOpt)]
enum Ticker {
  /// Query information about a specific ticker.
  #[structopt(name = "get")]
  Get {
    /// The ticker symbol to query information about.
    symbol: String,
  },
}


/// The handler for the 'ticker' command.
async fn ticker(client: Client, ticker: Ticker) -> Result<(), Error> {
  match ticker {
    Ticker::Get { symbol } => ticker_get(client, symbol).await,
  }
}


fn format_market(market: ticker::Market) -> &'static str {
  match market {
    ticker::Market::Stocks => "stocks",
    ticker::Market::Indices => "indices",
    ticker::Market::ForeignExchange => "fx",
  }
}


/// Retrieve and print a ticker.
async fn ticker_get(client: Client, symbol: String) -> Result<(), Error> {
  let response = client
    .issue::<ticker::Get>(symbol)
    .await
    .with_context(|| "failed to retrieve ticker information")?;

  let ticker = response
    .into_result()
    .with_context(|| "ticker response indicated non-success")?
    .ticker;

  println!(r#"{ticker}:
  name:      {name}
  market:    {market}
  locale:    {locale}
  currency:  {currency}
  active:    {active}"#,
    ticker = ticker.ticker,
    name = ticker.name,
    market = format_market(ticker.market),
    locale = ticker.locale,
    currency = ticker.currency,
    active = ticker.active,
  );
  Ok(())
}


async fn run() -> Result<(), Error> {
  let opts = Opts::from_args();
  let level = match opts.verbosity {
    0 => LevelFilter::WARN,
    1 => LevelFilter::INFO,
    2 => LevelFilter::DEBUG,
    _ => LevelFilter::TRACE,
  };

  let subscriber = FmtSubscriber::builder()
    .with_max_level(level)
    .with_timer(ChronoLocal::rfc3339())
    .finish();

  set_global_subscriber(subscriber).with_context(|| "failed to set tracing subscriber")?;

  let client =
    Client::from_env().with_context(|| "failed to retrieve Polygon environment information")?;

  match opts.command {
    Command::Ticker(ticker) => self::ticker(client, ticker).await,
  }
}

fn main() {
  let mut rt = Runtime::new().unwrap();
  let exit_code = rt
    .block_on(run())
    .map(|_| 0)
    .map_err(|e| {
      eprint!("{}", e);
      e.chain().skip(1).for_each(|cause| eprint!(": {}", cause));
      eprintln!();
    })
    .unwrap_or(1);
  // We exit the process the hard way next, so make sure to flush
  // buffered content.
  let _ = stdout().flush();
  exit(exit_code)
}
