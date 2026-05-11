use std::{env, sync::Arc};

use chrono::{DateTime, Local, NaiveDateTime, Utc};
use color_eyre::eyre::Result;
use rosu_v2::Osu;
use tokio::sync::Mutex;

use crate::{
    database::Database,
    types::{ApplicationCache, BarEntries, GraphEntries, SinglePointResponse, ratio},
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
        let database_url = env::var("DATABASE_URL")?;
        let client_id: u64 = env::var("OSU_API_CLIENT_ID")?.parse()?;
        let client_secret = env::var("OSU_API_CLIENT_SECRET")?;
        let mut database = Database::new(&database_url).await?;
        let client = rosu_v2::OsuBuilder::new()
            .client_id(client_id)
            .client_secret(client_secret)
            .ratelimit(1)
            .build()
            .await?;

        let cache = Self::build_cache(&client, &mut database).await?;

        Ok(Self {
            database: database,
            osu_client: client,
            cache,
        })
    }

    pub async fn build_cache(osu: &Osu, database: &mut Database) -> Result<ApplicationCache> {
        let initial_timestamp = Utc::now();
        let initial_changelog = fetch_changelog(osu).await?;
        database
            .insert_measurement(initial_changelog.clone().into())
            .await?;

        let peak_users_fut = database.get_user_count_peak();
        let peak_ratio_fut = database.get_user_ratio_peak();
        let peak_percentile_fut = database.get_user_highest_percentile_peak();

        let graph_day_users_fut = database.get_past_day();
        let graph_history_users_fut = database.get_history();

        let (peak_users, peak_ratio, peak_percentile, graph_day_users, graph_history_users) = tokio::join! {
            peak_users_fut, peak_ratio_fut, peak_percentile_fut, graph_day_users_fut, graph_history_users_fut
        };

        Ok(ApplicationCache {
            last_update: initial_timestamp,
            changelog_entries: BarEntries {
                latest: initial_changelog.into(),
                peak_user_count: peak_users?.into(),
                peak_user_percentage: peak_ratio?.into(),
                peak_percentile_percentage: peak_percentile?.into(),
            },
            graph_entries: GraphEntries {
                day_users: graph_day_users?.into(),
                history_users: graph_history_users?.into(),
            },
        })
    }

    pub async fn update_cache(&mut self) -> Result<()> {
        let current_timestamp = Utc::now();

        let initial_changelog = fetch_changelog(self.osu()).await?;
        self.insert_new_entry(initial_changelog.clone()).await?;

        let database = self.database();

        let peak_users_fut = database.get_user_count_peak();
        let peak_ratio_fut = database.get_user_ratio_peak();
        let peak_percentile_fut = database.get_user_highest_percentile_peak();

        let graph_day_users_fut = database.get_past_day();
        let graph_history_users_fut = database.get_history();

        let (peak_users, peak_ratio, peak_percentile, graph_day_users, graph_history_users) = tokio::join! {
            peak_users_fut, peak_ratio_fut, peak_percentile_fut, graph_day_users_fut, graph_history_users_fut
        };

        self.cache = ApplicationCache {
            last_update: current_timestamp,
            changelog_entries: BarEntries {
                latest: initial_changelog.into(),
                peak_user_count: peak_users?.into(),
                peak_user_percentage: peak_ratio?.into(),
                peak_percentile_percentage: peak_percentile?.into(),
            },
            graph_entries: GraphEntries {
                day_users: graph_day_users?.into(),
                history_users: graph_history_users?.into(),
            },
        };

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
    let stream = osu.changelog_listing().await?;

    let (stable, lazer) = stream
        .streams
        .into_iter()
        .filter(|s| STREAM_NAMES.contains(&s.name.as_str()))
        .map(|s| match s.name.as_str() {
            "stable40" | "cuttingedge" => Stream::Stable(s.user_count.unwrap_or(0)),
            "lazer" | "tachyon" => Stream::Lazer(s.user_count.unwrap_or(0)),
            stream => unreachable!("All valid stream names are matched, wtf is {stream}"),
        })
        .fold((0, 0), |(stable, lazer), stream| match stream {
            Stream::Stable(v) => (stable + v, lazer),
            Stream::Lazer(v) => (stable, lazer + v),
        });

    let entries = SinglePointResponse {
        timestamp: Local::now(),
        stable,
        lazer,
        ratio: ratio(stable, lazer),
        sum: stable + lazer,
    };

    Ok(entries)
}
