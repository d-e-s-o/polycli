// Copyright (C) 2020 Daniel Mueller <deso@posteo.net>
// SPDX-License-Identifier: GPL-3.0-or-later

use std::borrow::Cow;
use std::convert::TryInto;
use std::io::stdout;
use std::io::Write;
use std::iter::empty;
use std::process::exit;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Error;

use chrono::offset::Local;
use chrono::offset::TimeZone;

use futures::TryStreamExt;

use polyio::api::ticker;
use polyio::Client;
use polyio::Event;
use polyio::Stock;
use polyio::Subscription;

use serde_json::to_string as to_json;

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
  /// Subscribe to ticker events.
  #[structopt(name = "events")]
  Events(Events),
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


/// Parse a "stock" symbol.
///
/// We support a sentinel value, "all", that is treated specially.
fn parse_stock(s: &str) -> Result<Stock, Error> {
  let stock = match s {
    "all" => Stock::All,
    s => {
      s.as_bytes().iter().try_fold((), |(), c| {
        if !c.is_ascii_alphabetic() || !c.is_ascii_uppercase() {
          let err = anyhow!("encountered unexpected character '{}'", *c as char);
          Err(err).with_context(|| "invalid stock symbol")
        } else {
          Ok(())
        }
      })?;

      Stock::Symbol(s.to_string().into())
    },
  };

  Ok(stock)
}


/// An enumeration representing the `events` command.
#[derive(Debug, StructOpt)]
struct Events {
  /// Subscribe to trades for the given stock.
  #[structopt(short = "t", long = "trades", parse(try_from_str = parse_stock))]
  trades: Vec<Stock>,
  /// Subscribe to quotes for the given stock.
  #[structopt(short = "q", long = "quotes", parse(try_from_str = parse_stock))]
  quotes: Vec<Stock>,
  /// Subscribe to second aggregates for the given stock.
  #[structopt(short = "s", long = "secondly", parse(try_from_str = parse_stock))]
  secondly: Vec<Stock>,
  /// Subscribe to second aggregates for the given stock.
  #[structopt(short = "m", long = "minutely", parse(try_from_str = parse_stock))]
  minutely: Vec<Stock>,
  /// Print events in JSON format.
  #[structopt(short = "j", long = "json")]
  json: bool,
}


/// Format a system time as per RFC 2822.
// TODO: Perhaps it makes more sense to format time in the Eastern time
//       zone?
fn format_time(time: &SystemTime) -> Cow<'static, str> {
  match time.duration_since(UNIX_EPOCH) {
    Ok(duration) => {
      let secs = duration.as_secs().try_into().unwrap();
      let nanos = duration.subsec_nanos();
      Local.timestamp(secs, nanos).to_rfc2822().into()
    },
    Err(..) => "N/A".into(),
  }
}


fn print_events(events: &[Event]) {
  for event in events {
    match event {
      Event::SecondAggregate(aggregate) |
      Event::MinuteAggregate(aggregate) => {
        println!(r#"{symbol} aggregate:
  start time:          {start_time}
  end time:            {end_time}
  open price today:    {open_price_today}
  volume:              {volume}
  accumulated volume:  {acc_volume}
  tick open price:     {open_price}
  tick close price:    {close_price}
  tick low price:      {low_price}
  tick high price:     {high_price}
  tick avg price:      {avg_price}"#,
          symbol = aggregate.symbol,
          start_time = format_time(&aggregate.start_timestamp),
          end_time = format_time(&aggregate.end_timestamp),
          open_price_today = aggregate.open_price_today,
          volume = aggregate.volume,
          acc_volume = aggregate.accumulated_volume,
          open_price = aggregate.open_price,
          close_price = aggregate.close_price,
          low_price = aggregate.low_price,
          high_price = aggregate.high_price,
          avg_price = aggregate.average_price,
        );
      },
      Event::Trade(trade) => {
        // TODO: We may also want to decode and print the exchange and the conditions.
        println!(r#"{symbol} trade:
  timestamp:  {time}
  price:      {price}
  quantity:   {quantity}"#,
          symbol = trade.symbol,
          time = format_time(&trade.timestamp),
          price = trade.price,
          quantity = trade.quantity,
        );
      },
      Event::Quote(quote) => {
        println!(r#"{symbol} quote:
  timestamp:     {time}
  bid price:     {bid_price}
  bid quantity:  {bid_quantity}
  ask price:     {ask_price}
  ask quantity:  {ask_quantity}"#,
          symbol = quote.symbol,
          time = format_time(&quote.timestamp),
          bid_price = quote.bid_price,
          bid_quantity = quote.bid_quantity,
          ask_price = quote.ask_price,
          ask_quantity = quote.ask_quantity,
        );
      },
    }
  }
}


/// The handler for the 'events' command.
async fn events(client: Client, events: Events) -> Result<(), Error> {
  let json = events.json;
  let subscriptions = empty()
    .chain(events.trades.into_iter().map(Subscription::Trades))
    .chain(events.quotes.into_iter().map(Subscription::Quotes))
    .chain(
      events
        .secondly
        .into_iter()
        .map(Subscription::SecondAggregates),
    )
    .chain(
      events
        .minutely
        .into_iter()
        .map(Subscription::MinuteAggregates),
    );

  client
    .subscribe(subscriptions)
    .await
    .with_context(|| "failed to subscribe to ticker updates")?
    .map_err(Error::from)
    .try_for_each(|result| {
      async {
        let events = result.unwrap();
        if json {
          let json =
            to_json(&events).with_context(|| "failed to serialize ticker event to JSON")?;
          println!("{}", json);
        } else {
          print_events(&events);
        }
        Ok(())
      }
    })
    .await?;

  Ok(())
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
    Command::Events(events) => self::events(client, events).await,
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
