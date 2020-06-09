#![feature(try_blocks)]
use clap::Clap;
use serde::Deserialize;
use std::error::Error;
use std::{
    fs,
    sync::{Arc, Mutex},
};
use threadpool::ThreadPool;

/// Reads csv files exported from Google Play Music takeout and prints your most
/// listened songs
#[derive(Clap)]
#[clap(version = "1.0", author = "Rasmus E. <fruitiex@gmail.com>")]
struct Args {
    /// Directory where to read csv files from
    dir: String,

    /// Number of threads to use
    #[clap(short, long, default_value = "8")]
    jobs: usize,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(deserialize = "PascalCase"))]
struct Song {
    title: String,
    album: String,
    artist: String,
    #[serde(alias = "Duration (ms)")]
    duration_ms: u64,
    rating: u32,
    #[serde(alias = "Play Count")]
    play_count: u64,
    removed: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Args = Args::parse();
    let files = fs::read_dir(args.dir)?;
    let pool = ThreadPool::new(args.jobs);
    println!("using {} threads", args.jobs);

    let songs: Arc<Mutex<Vec<Song>>> = Arc::new(Mutex::new(vec![]));

    for entry in files {
        let file: fs::DirEntry = entry?;
        let path = file.path();
        let path_str = path.to_str().unwrap();

        // skip non-csv filename extensions
        if !path_str.ends_with(".csv") {
            continue;
        };

        let songs = songs.clone();
        pool.execute(move || {
            let _result: Result<(), Box<dyn Error>> = try {
                let mut reader = csv::Reader::from_path(path)?;
                let results = reader.deserialize();

                // lock mutex for writing new items into songs
                let mut songs = songs.lock()?;

                for result in results {
                    songs.push(result?);
                }
            };
        })
    }

    // wait until threads are done with work
    pool.join();

    // unwrap songs from the Arc<Mutex<...>> since we're left with only one thread
    let songs = Arc::try_unwrap(songs).unwrap().into_inner()?;
    let songs_count = songs.len();

    // filter out never played songs
    let mut played_songs: Vec<Song> = songs.into_iter().filter(|t| t.play_count > 0).collect();
    let played_songs_count = played_songs.len();

    // sort most listened first
    played_songs.sort_unstable_by_key(|r| r.play_count);
    played_songs.reverse();

    for (index, track) in played_songs.iter().enumerate() {
        println!(
            "#{}, (play count {}): {} - {} ({})",
            index + 1,
            track.play_count,
            track.artist,
            track.title,
            track.album,
        );
    }

    println!(
        "done reading {} songs, found {} matches",
        songs_count, played_songs_count
    );

    Ok(())
}
