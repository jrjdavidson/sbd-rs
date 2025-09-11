//! Receive incoming Iridium messages through their Direct IP service.
//!
//! Iridium `DirectIP` is a service provided by the Iridium company. New Mobile
//! Originated messages are forwarded from the Iridium servers to a configured
//! IP address. The Iridium service attempts to initiate a TCP connection to port
//! 10800 at the specified IP. If the connection is successful, the MO message
//! is transmitted, then the connection is closed.
//!
//! This module provides a `Server` structure, which can be created to run
//! forever and receive those incoming MO messages.

use log::{debug, error, info, warn};
use std::{
    fs,
    io::{self, Cursor, Read},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{Arc, Mutex},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{mo::Message, storage::Storage};

/// A Iridium `DirectIP` server.
///
/// The server will listen on a socket address for incoming Iridium SBD Mobile Originated
/// messages. Incoming messages will be stored using `sbd::filesystem::Storage`. Errors are logged
/// using the logging framework.
#[derive(Debug)]
pub struct Server<A: ToSocketAddrs + Sync, S: Storage + Sync + Send> {
    addr: A,
    listener: Option<TcpListener>,
    storage: Arc<Mutex<S>>,
}

impl<A, S> Server<A, S>
where
    A: ToSocketAddrs + Sync,
    S: 'static + Storage + Sync + Send,
{
    /// Creates a new server that will listen on `addr` and write messages to `storage`.
    ///
    /// This method does not actually bind to the socket address or do anything with the storage.
    /// Use `bind` and `serve_forever` to actually do stuff.
    ///
    /// The provided storage is expected to be ready to accept new messages.
    ///
    /// # Examples
    ///
    /// ```
    /// let storage = sbd::storage::MemoryStorage::new();
    /// let server = sbd::directip::Server::new("0.0.0.0:10800", storage);
    /// ```
    pub fn new(addr: A, storage: S) -> Server<A, S> {
        Server {
            addr,
            listener: None,
            storage: Arc::new(Mutex::new(storage)),
        }
    }

    /// Binds this server to its tcp socket.
    ///
    /// This is a seperate operation from `serve_forever` so that we can capture any errors
    /// associated with the underlying `TcpListener::bind`.
    ///
    /// # Examples
    ///
    /// ```
    /// let storage = sbd::storage::MemoryStorage::new();
    /// let mut server = sbd::directip::Server::new("0.0.0.0:10800", storage);
    /// server.bind().unwrap();
    /// ```
    pub fn bind(&mut self) -> io::Result<()> {
        self.listener = Some(self.create_listener()?);
        Ok(())
    }

    /// Starts the DirectIP server and serves forever.
    ///
    /// # Panics
    ///
    /// This method panics if it has a problem binding to the tcp socket address. To avoid a panic,
    /// use `Server::bind` before calling `Server::serve_forever`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let storage = sbd::storage::MemoryStorage::new();
    /// let mut server = sbd::directip::Server::new("0.0.0.0:10800", storage);
    /// server.bind().unwrap();
    /// server.serve_forever();
    /// ```
    pub fn serve_forever(mut self) {
        let listener = match self.listener {
            Some(ref listener) => listener,
            None => {
                self.listener = Some(self.create_listener().unwrap());
                self.listener.as_ref().unwrap()
            }
        };
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let storage = Arc::clone(&self.storage);
                    thread::spawn(move || handle_stream(stream, storage));
                }
                Err(err) => {
                    thread::spawn(move || handle_error(&err));
                }
            }
        }
    }

    fn create_listener(&self) -> io::Result<TcpListener> {
        TcpListener::bind(&self.addr)
    }
}

fn handle_stream(mut stream: TcpStream, storage: Arc<Mutex<dyn Storage>>) {
    let peer = stream.peer_addr().ok();

    // (1) Read the whole DirectIP message. For MO, Iridium closes after one message.
    // Add a read timeout if you like:
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(15)));

    let mut buf = Vec::new();
    if let Err(err) = stream.read_to_end(&mut buf) {
        error!("Error reading DirectIP stream: {:?}", err);
        return;
    }

    // (2) Save raw bytes for testcases.
    // Put IMEI in the filename later (after parse); for now, use a timestamp.
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let fname = format!("captures/{}-{:?}.mo.sbd", ts, peer);
    if let Err(err) = fs::create_dir_all("captures").and_then(|_| fs::write(&fname, &buf)) {
        warn!("Could not save DirectIP capture to {}: {:?}", fname, err);
    } else {
        debug!("Saved DirectIP capture to {}", fname);
    }

    // (3) Parse from a Cursor over the saved bytes.
    let mut cur = Cursor::new(&buf);
    let message = match Message::read_from(&mut cur) {
        Ok(msg) => {
            info!(
                "Received message from IMEI {} with MOMSN {} and {} byte payload",
                msg.imei(),
                msg.momsn(),
                msg.payload().len(),
            );
            // Optional: rename the file to include IMEI now that we know it
            let new_name = format!("captures/{}-{}-{}.mo.sbd", ts, msg.imei(), msg.momsn());
            if let Err(e) = fs::rename(&fname, &new_name) {
                debug!("Rename {} -> {} failed: {:?}", fname, new_name, e);
            }
            msg
        }
        Err(err) => {
            error!("Error when reading message: {:?}", err);
            // keep the saved .sbd file for debugging
            return;
        }
    };

    // (4) Store via your Storage backend.
    if let Err(err) = storage.lock().expect("lock").store(message) {
        error!("Problem storing message: {:?}", err);
    } else {
        info!("Stored message");
    }
}

/// Handles an error when handling a connection.
fn handle_error(err: &io::Error) {
    error!("Error when receiving tcp communication: {:?}", err);
}
