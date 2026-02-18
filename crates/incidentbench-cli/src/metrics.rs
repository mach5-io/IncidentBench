// Copyright 2025 Mach5 Software, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use incidentbench_common::proto::aggregator::{
    metrics_service_client::MetricsServiceClient, AggregatedSnapshot, StreamMetricsRequest,
};
use tonic::transport::Channel;

/// Stream metrics as structured lines (table or JSON).
pub async fn stream(run_name: &str, namespace: &str, format: &str) -> anyhow::Result<()> {
    let addr = discover_aggregator_addr(run_name, namespace).await?;
    let channel = Channel::from_shared(format!("http://{}", addr))?
        .connect()
        .await?;
    let mut client = MetricsServiceClient::new(channel);

    let request = StreamMetricsRequest {
        include_history: false,
    };
    let mut stream = client.stream_metrics(request).await?.into_inner();

    // Print header for table format.
    if format == "table" {
        println!(
            "{:<12} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8} {:>12} {:>6}",
            "PHASE", "INGEST_EPS", "TARGET", "Q_QPS", "Q_TARG", "P50", "P99", "KAFKA_LAG", "ERRS"
        );
    }

    while let Some(snapshot) = stream.message().await? {
        match format {
            "json" => {
                println!("{}", format_snapshot_json(&snapshot));
            }
            _ => {
                println!("{}", format_snapshot_table(&snapshot));
            }
        }
    }

    Ok(())
}

/// Live terminal UI with ratatui.
pub async fn live_tui(run_name: &str, namespace: &str) -> anyhow::Result<()> {
    use crossterm::{
        event::{self, Event, KeyCode},
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };
    use ratatui::{
        backend::CrosstermBackend,
        layout::{Constraint, Direction, Layout},
        style::{Color, Style},
        text::{Line, Span},
        widgets::{Block, Borders, Paragraph, Sparkline},
        Terminal,
    };
    use std::io;
    use std::sync::{Arc, Mutex};

    let addr = discover_aggregator_addr(run_name, namespace).await?;
    let channel = Channel::from_shared(format!("http://{}", addr))?
        .connect()
        .await?;
    let mut client = MetricsServiceClient::new(channel);

    let request = StreamMetricsRequest {
        include_history: true,
    };
    let mut grpc_stream = client.stream_metrics(request).await?.into_inner();

    // Shared state for TUI.
    let latest = Arc::new(Mutex::new(None::<AggregatedSnapshot>));
    let history = Arc::new(Mutex::new(Vec::<AggregatedSnapshot>::new()));

    // Spawn metrics receiver.
    let latest_clone = latest.clone();
    let history_clone = history.clone();
    tokio::spawn(async move {
        while let Ok(Some(snapshot)) = grpc_stream.message().await {
            {
                let mut h = history_clone.lock().unwrap();
                h.push(snapshot.clone());
                // Keep last 300 seconds.
                if h.len() > 300 {
                    let excess = h.len() - 300;
                    h.drain(0..excess);
                }
            }
            *latest_clone.lock().unwrap() = Some(snapshot);
        }
    });

    // Setup terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),  // Title
                    Constraint::Length(5),  // Phase + Progress
                    Constraint::Length(5),  // Scorecard row 1
                    Constraint::Length(5),  // Scorecard row 2
                    Constraint::Min(8),    // Sparklines
                    Constraint::Length(3),  // Footer
                ])
                .split(f.area());

            // Title.
            let title = Paragraph::new(Line::from(vec![
                Span::styled(
                    "IncidentBench ",
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(
                    run_name,
                    Style::default().fg(Color::White),
                ),
            ]))
            .block(Block::default().borders(Borders::BOTTOM));
            f.render_widget(title, chunks[0]);

            let snap = latest.lock().unwrap().clone();

            if let Some(ref s) = snap {
                // Phase + Progress bar.
                let _total_elapsed = s.timestamp_ns / 1_000_000_000;
                let _progress_ratio = 0.5f64; // Placeholder
                let phase_info = format!(
                    "Phase: {}  |  Ingest: {} / {} EPS  |  Query: {:.1} / {:.1} QPS  |  Kafka Lag: {}",
                    s.phase,
                    s.ingest_events_produced,
                    s.ingest_target_eps,
                    s.query_executed as f64,
                    s.query_target_qps,
                    s.kafka_consumer_lag,
                );
                let info = Paragraph::new(phase_info)
                    .block(Block::default().borders(Borders::ALL).title("Status"));
                f.render_widget(info, chunks[1]);

                // Metrics cards row 1.
                let cards1 = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Percentage(25),
                        Constraint::Percentage(25),
                        Constraint::Percentage(25),
                        Constraint::Percentage(25),
                    ])
                    .split(chunks[2]);

                let latency = s.query_latency.as_ref();
                let _ingest_lat = s.ingest_kafka_produce_latency.as_ref();

                let eps_widget = Paragraph::new(format!("{}", s.ingest_events_produced))
                    .block(Block::default().borders(Borders::ALL).title("Ingest EPS"));
                f.render_widget(eps_widget, cards1[0]);

                let qps_widget = Paragraph::new(format!("{}", s.query_executed))
                    .block(Block::default().borders(Borders::ALL).title("Query QPS"));
                f.render_widget(qps_widget, cards1[1]);

                let p99_val = latency.map(|l| l.p99).unwrap_or(0.0);
                let p99_widget = Paragraph::new(format!("{:.1} ms", p99_val))
                    .block(Block::default().borders(Borders::ALL).title("Query p99"));
                f.render_widget(p99_widget, cards1[2]);

                let lag_widget = Paragraph::new(format!("{}", s.kafka_consumer_lag))
                    .block(Block::default().borders(Borders::ALL).title("Kafka Lag"));
                f.render_widget(lag_widget, cards1[3]);

                // Metrics cards row 2.
                let cards2 = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Percentage(25),
                        Constraint::Percentage(25),
                        Constraint::Percentage(25),
                        Constraint::Percentage(25),
                    ])
                    .split(chunks[3]);

                let p50_val = latency.map(|l| l.p50).unwrap_or(0.0);
                let p50_widget = Paragraph::new(format!("{:.1} ms", p50_val))
                    .block(Block::default().borders(Borders::ALL).title("Query p50"));
                f.render_widget(p50_widget, cards2[0]);

                let p95_val = latency.map(|l| l.p95).unwrap_or(0.0);
                let p95_widget = Paragraph::new(format!("{:.1} ms", p95_val))
                    .block(Block::default().borders(Borders::ALL).title("Query p95"));
                f.render_widget(p95_widget, cards2[1]);

                let errors_widget = Paragraph::new(format!("{}", s.query_errors))
                    .block(Block::default().borders(Borders::ALL).title("Errors"));
                f.render_widget(errors_widget, cards2[2]);

                let workers_widget = Paragraph::new(format!(
                    "I:{} Q:{}",
                    s.ingest_workers_reporting, s.query_workers_reporting
                ))
                .block(Block::default().borders(Borders::ALL).title("Workers"));
                f.render_widget(workers_widget, cards2[3]);

                // Sparklines.
                let hist = history.lock().unwrap();
                let eps_data: Vec<u64> = hist.iter().rev().take(60).rev()
                    .map(|s| s.ingest_events_produced as u64)
                    .collect();
                let sparkline = Sparkline::default()
                    .block(Block::default().borders(Borders::ALL).title("Ingest EPS (60s)"))
                    .data(&eps_data)
                    .style(Style::default().fg(Color::Cyan));
                f.render_widget(sparkline, chunks[4]);
            } else {
                let waiting = Paragraph::new("Waiting for metrics...")
                    .block(Block::default().borders(Borders::ALL));
                f.render_widget(waiting, chunks[1]);
            }

            // Footer.
            let footer = Paragraph::new("Press 'q' to quit")
                .style(Style::default().fg(Color::DarkGray));
            f.render_widget(footer, chunks[5]);
        })?;

        // Check for quit key.
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                    break;
                }
            }
        }
    }

    // Restore terminal.
    disable_raw_mode()?;
    crossterm::execute!(io::stdout(), LeaveAlternateScreen)?;

    Ok(())
}

fn format_snapshot_table(s: &AggregatedSnapshot) -> String {
    let latency = s.query_latency.as_ref();
    format!(
        "{:<12} {:>10} {:>10} {:>8} {:>8.1} {:>8.1} {:>8.1} {:>12} {:>6}",
        s.phase,
        s.ingest_events_produced,
        s.ingest_target_eps,
        s.query_executed,
        s.query_target_qps,
        latency.map(|l| l.p50).unwrap_or(0.0),
        latency.map(|l| l.p99).unwrap_or(0.0),
        s.kafka_consumer_lag,
        s.query_errors,
    )
}

fn format_snapshot_json(s: &AggregatedSnapshot) -> String {
    let latency = s.query_latency.as_ref();
    serde_json::json!({
        "phase": s.phase,
        "ingest_eps": s.ingest_events_produced,
        "ingest_target_eps": s.ingest_target_eps,
        "query_qps": s.query_executed,
        "query_target_qps": s.query_target_qps,
        "query_p50": latency.map(|l| l.p50).unwrap_or(0.0),
        "query_p95": latency.map(|l| l.p95).unwrap_or(0.0),
        "query_p99": latency.map(|l| l.p99).unwrap_or(0.0),
        "kafka_consumer_lag": s.kafka_consumer_lag,
        "query_errors": s.query_errors,
    })
    .to_string()
}

/// Discover the MetricsAggregator address for a given run.
/// Tries kubectl port-forward first, then falls back to direct service address.
async fn discover_aggregator_addr(run_name: &str, namespace: &str) -> anyhow::Result<String> {
    // Try direct service address first.
    let svc_addr = format!("{}-aggregator.{}.svc:50052", run_name, namespace);

    // In a real implementation, we'd try to connect to the service directly,
    // and if that fails (e.g., running outside the cluster), set up kubectl port-forward.
    // For now, return the service address.
    Ok(svc_addr)
}
