use std::{env, sync::Arc};

use chrono::{Local, Utc};
use color_eyre::eyre::Result;
use rosu_v2::Osu;
use tokio::sync::Mutex;

use crate::{
    database::Database,
    types::{
        self, ApplicationCache, BarEntries, BucketSize, GraphEntries, SinglePointResponse, ratio,
    },
};

const STREAM_NAMES: [&str; 4] = ["stable40", "cuttingedge", "lazer", "tachyon"];

enum Stream {
    Stable(i64),
    Lazer(i64),
}

pub struct Server {
    database: Database,
    osu_client: Osu,
    cache: ApplicationCache,
}

pub type ServerState = Arc<Mutex<Server>>;

impl Server {
    pub async fn init() -> Result<Self> {
        tracing::info!("Initializing server dependencies");

        let database_url = env::var("DATABASE_URL")?;
        let client_id: u64 = env::var("OSU_API_CLIENT_ID")?.parse()?;
        let client_secret = env::var("OSU_API_CLIENT_SECRET")?;

        tracing::debug!("Connecting to database");
        let mut database = Database::new(&database_url).await?;
        // Run migrations owo
        cfg_select! {
            not(debug_assertions) => {
                database.migrate().await?;
            }
            _ => {}
        }

        tracing::debug!(client_id, "Building osu! API client");
        let client = rosu_v2::OsuBuilder::new()
            .client_id(client_id)
            .client_secret(client_secret)
            .ratelimit(1)
            .build()
            .await?;

        tracing::info!("Building initial application cache");
        let cache = Self::build_cache(&client, &mut database).await?;

        tracing::info!("Server initialization complete");

        Ok(Self {
            database: database,
            osu_client: client,
            cache,
        })
    }

    #[tracing::instrument(skip_all)]
    pub async fn build_cache(osu: &Osu, database: &mut Database) -> Result<ApplicationCache> {
        tracing::info!("Refreshing application cache data");
        let initial_timestamp = Utc::now();
        let initial_changelog = fetch_changelog(osu).await?;

        tracing::debug!(
            timestamp = initial_changelog.timestamp,
            stable = initial_changelog.stable,
            lazer = initial_changelog.lazer,
            "Fetched changelog measurement for cache"
        );

        database
            .insert_measurement(initial_changelog.clone().into())
            .await?;

        let peak_users_fut = database.get_user_count_peak();
        let peak_ratio_fut = database.get_user_ratio_peak();
        let peak_percentile_fut = database.get_user_highest_percentile_peak();

        let graph_day_users_fut = database.get_past_day();
        let graph_history_users_fut = database.get_history(BucketSize::Day);

        let (peak_users, peak_ratio, peak_percentile, graph_day_users, graph_history_users) = tokio::join! {
            peak_users_fut, peak_ratio_fut, peak_percentile_fut, graph_day_users_fut, graph_history_users_fut
        };

        let peak_users = peak_users?;
        let peak_ratio = peak_ratio?;
        let peak_percentile = peak_percentile?;
        let graph_day_users = graph_day_users?;
        let graph_history_users = graph_history_users?;

        tracing::info!(
            day_points = graph_day_users.len(),
            history_points = graph_history_users.len(),
            peak_users_timestamp = peak_users.timestamp,
            peak_ratio_timestamp = peak_ratio.timestamp,
            peak_percentile_timestamp = peak_percentile.timestamp,
            "Application cache data loaded"
        );

        Ok(ApplicationCache {
            last_update: initial_timestamp,
            changelog_entries: BarEntries {
                latest: initial_changelog.into(),
                peak_user_count: peak_users.into(),
                peak_user_percentage: peak_ratio.into(),
                peak_percentile_percentage: peak_percentile.into(),
            },
            graph_entries: GraphEntries {
                day_users: graph_day_users.into(),
                history_users: graph_history_users.into(),
            },
        })
    }

    pub async fn update_cache(&mut self) -> Result<()> {
        tracing::info!("Starting application cache update");
        let current_timestamp = Utc::now();

        let initial_changelog = fetch_changelog(self.osu()).await?;
        self.insert_new_entry(initial_changelog.clone()).await?;

        let database = self.database();

        let peak_users_fut = database.get_user_count_peak();
        let peak_ratio_fut = database.get_user_ratio_peak();
        let peak_percentile_fut = database.get_user_highest_percentile_peak();

        let graph_day_users_fut = database.get_past_day();
        let graph_history_users_fut = database.get_history(BucketSize::Day);

        let (peak_users, peak_ratio, peak_percentile, graph_day_users, graph_history_users) = tokio::join! {
            peak_users_fut, peak_ratio_fut, peak_percentile_fut, graph_day_users_fut, graph_history_users_fut
        };

        let peak_users = peak_users?;
        let peak_ratio = peak_ratio?;
        let peak_percentile = peak_percentile?;
        let graph_day_users = graph_day_users?;
        let graph_history_users = graph_history_users?;

        tracing::info!(
            timestamp = current_timestamp.timestamp(),
            latest_stable = initial_changelog.stable,
            latest_lazer = initial_changelog.lazer,
            day_points = graph_day_users.len(),
            history_points = graph_history_users.len(),
            "Application cache update data loaded"
        );

        self.cache = ApplicationCache {
            last_update: current_timestamp,
            changelog_entries: BarEntries {
                latest: initial_changelog.into(),
                peak_user_count: peak_users.into(),
                peak_user_percentage: peak_ratio.into(),
                peak_percentile_percentage: peak_percentile.into(),
            },
            graph_entries: GraphEntries {
                day_users: graph_day_users.into(),
                history_users: graph_history_users.into(),
            },
        };

        tracing::info!("Application cache update complete");

        Ok(())
    }

    pub fn database(&mut self) -> &mut Database {
        &mut self.database
    }

    pub fn osu(&self) -> &Osu {
        &self.osu_client
    }

    pub fn cache(&self) -> &ApplicationCache {
        &self.cache
    }

    #[tracing::instrument(fields(stale = self.cache.is_stale()), skip(self))]
    pub async fn get_latest_changelog(&self) -> Result<SinglePointResponse> {
        let stream: SinglePointResponse = self.cache.latest().clone().into();

        tracing::info!(
            stable = stream.stable,
            lazer = stream.lazer,
            "Fetched latest changelog"
        );
        Ok(stream)
    }

    pub fn insert_new_entry(
        &mut self,
        entry: SinglePointResponse,
    ) -> impl Future<Output = Result<()>> {
        self.database.insert_measurement(entry.into())
    }
}

pub async fn fetch_changelog(osu: &Osu) -> Result<SinglePointResponse> {
    tracing::debug!("Fetching changelog stream data from osu! API");
    let stream = osu.changelog_listing().await?;
    let stream_count = stream.streams.len();

    let (stable, lazer) = stream
        .streams
        .into_iter()
        .filter(|s| STREAM_NAMES.contains(&s.name.as_str()))
        .map(|s| match s.name.as_str() {
            "stable40" | "cuttingedge" => {
                let user_count = match s.user_count {
                    Some(user_count) => user_count,
                    None => {
                        tracing::warn!(stream = %s.name, "Changelog stream missing user count");
                        0
                    }
                };
                Stream::Stable(user_count)
            }
            "lazer" | "tachyon" => {
                let user_count = match s.user_count {
                    Some(user_count) => user_count,
                    None => {
                        tracing::warn!(stream = %s.name, "Changelog stream missing user count");
                        0
                    }
                };
                Stream::Lazer(user_count)
            }
            stream => unreachable!("All valid stream names are matched, wtf is {stream}"),
        })
        .fold((0, 0), |(stable, lazer), stream| match stream {
            Stream::Stable(v) => (stable + v, lazer),
            Stream::Lazer(v) => (stable, lazer + v),
        });

    let entries = SinglePointResponse {
        timestamp: Local::now().timestamp(),
        stable,
        lazer,
        ratio: ratio(stable, lazer),
        sum: stable + lazer,
    };

    tracing::info!(
        timestamp = entries.timestamp,
        stable = entries.stable,
        lazer = entries.lazer,
        sum = entries.sum,
        ratio = entries.ratio,
        stream_count,
        "Fetched changelog user counts"
    );

    Ok(entries)
}
