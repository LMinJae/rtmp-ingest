use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::SystemTime;
use bytes::{Buf, BufMut, BytesMut};
use byteorder::{BigEndian, WriteBytesExt};

use rtmp;
use isobmff::IO;

// https://doc.rust-lang.org/book/ch20-03-graceful-shutdown-and-cleanup.html
mod thread_pool {
    use std::thread;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Mutex;

    enum Message {
        New(Box<dyn FnOnce() + Send + 'static>),
        Terminate,
    }

    struct Worker {
        thread: Option<thread::JoinHandle<()>>,
    }

    impl Worker {
        pub fn new(rx: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
            let thread = thread::spawn(move || loop {
                match rx.lock().unwrap().recv().unwrap() {
                    Message::New(job) => {
                        job();
                    }
                    Message::Terminate => {
                        break;
                    }
                }
            });

            Worker {
                thread: Some(thread),
            }
        }
    }

    pub(crate) struct ThreadPool {
        workers: Vec<Worker>,
        tx: mpsc::Sender<Message>,
    }

    impl ThreadPool {
        pub fn new(size: usize) -> Self {
            let (tx, rx) = mpsc::channel();
            let rx = Arc::new(Mutex::new(rx));

            let mut workers = Vec::with_capacity(size);
            for _ in 0..size {
                workers.push(Worker::new(Arc::clone(&rx)));
            }

            ThreadPool {
                workers,
                tx,
            }
        }

        pub fn spawn<F>(&self, f: F)
            where
                F: FnOnce() + Send + 'static,
        {
            self.tx.send(Message::New(Box::new(f))).unwrap();
        }
    }

    impl Drop for ThreadPool {
        fn drop(&mut self) {
            for _ in &self.workers {
                self.tx.send(Message::Terminate).unwrap();
            }

            for w in &mut self.workers {
                if let Some(t) = w.thread.take() {
                    t.join().unwrap();
                }
            }
        }
    }
}

struct MediaStream {
    key: String,

    f_v: File,
    f_a: File,

    f_playlist: File,

    samplerate: u32,
    moov: isobmff::moov::moov,
    need_write_init_seg: bool,
    sequence_number: u32,
    framerate: u32,

    trun_v: Vec<(u32, u32)>,
    data_v: BytesMut,
    trun_a: Vec<u32>,
    data_a: BytesMut,
}

impl Drop for MediaStream {
    fn drop(&mut self) {
        self.flush_segment();
    }
}

impl MediaStream {
    pub fn new(key: String) -> Self {
        std::fs::create_dir_all(
            std::path::Path::new(
                format!("./{}/", key.to_owned()).as_str()
            )).unwrap();

        Self {
            key: key.to_owned(),

            f_v: File::create(format!("./{}/dump.h264", key.to_owned())).unwrap(),
            f_a: File::create(format!("./{}/dump.aac", key.to_owned())).unwrap(),

            f_playlist: File::create(format!("./{}/prog_index.m3u8", key.to_owned())).unwrap(),

            samplerate: 0,
            moov: isobmff::moov::moov::default(),
            need_write_init_seg: true,
            sequence_number: 0,
            framerate: 30,

            trun_v: vec![],
            data_v: Default::default(),
            trun_a: vec![],
            data_a: Default::default(),
        }
    }

    fn process(&mut self, msg: rtmp::message::Message) {
        match msg {
            rtmp::message::Message::Data { payload } => {
                let p0 = {
                    match &payload[0] {
                        amf::Value::Amf0Value(amf::amf0::Value::String(str)) => str.as_str(),
                        _ => {
                            eprintln!("Unexpected {:?}", payload);
                            return
                        }
                    }
                };
                match p0 {
                    "@setDataFrame" => {
                        let p1 = {
                            match &payload[1] {
                                amf::Value::Amf0Value(amf::amf0::Value::String(str)) => str.as_str(),
                                _ => {
                                    eprintln!("Unexpected {:?}", payload);
                                    return
                                }
                            }
                        };
                        let p2 = {
                            match &payload[2] {
                                amf::Value::Amf0Value(amf::amf0::Value::ECMAArray(arr)) => arr,
                                _ => {
                                    eprintln!("Unexpected {:?}", payload);
                                    return
                                }
                            }
                        };
                        eprintln!("{:?} {:?} {:?}", p0, p1, p2);

                        self.samplerate = if let Some(amf::amf0::Value::Number(n)) = p2.get("audiosamplerate") {
                            *n as u32
                        } else { 0 };

                        self.framerate = if let Some(amf::amf0::Value::Number(n)) = p2.get("framerate") {
                            *n as u32
                        } else { 30 };

                        self.moov.mvhd.timescale = 1000;

                        self.moov.traks.push({
                            let mut trak = isobmff::moov::trak::default();

                            trak.tkhd.track_id = 1;
                            trak.tkhd.alternate_group = 0;
                            trak.tkhd.volume = 0;
                            trak.tkhd.width = if let Some(amf::amf0::Value::Number(n)) = p2.get("width") {
                                (*n as u32) << 16
                            } else { 0 };
                            trak.tkhd.height = if let Some(amf::amf0::Value::Number(n)) = p2.get("height") {
                                (*n as u32) << 16
                            } else { 0 };

                            trak.mdia.mdhd.timescale = 1000;

                            trak.mdia.hdlr = isobmff::moov::hdlr::vide("VideoHandler");

                            trak.mdia.minf.mhd = isobmff::moov::MediaInformationHeader::vmhd(isobmff::moov::vmhd::new(0, 0, 0, 0));

                            trak
                        });

                        self.moov.traks.push({
                            let mut trak = isobmff::moov::trak::default();

                            trak.tkhd.track_id = 2;
                            trak.tkhd.alternate_group = 1;

                            trak.mdia.mdhd.timescale = self.samplerate;

                            trak.mdia.hdlr = isobmff::moov::hdlr::soun("SoundHandler");

                            trak.mdia.minf.mhd = isobmff::moov::MediaInformationHeader::smhd(isobmff::moov::smhd::new(0));

                            trak
                        });

                        for trak in self.moov.traks.iter_mut() {
                            self.moov.mvex.trexs.push({
                                let mut trex = isobmff::moov::trex::default();

                                trex.track_id = trak.tkhd.track_id;
                                trex.default_sample_description_index = 1;

                                trex
                            });
                        }
                    }
                    _ => {
                        eprintln!("Unexpected {}: {:?}", p0, payload);
                    }
                };
            }
            rtmp::message::Message::Audio { dts: _dts, control, mut payload } => {
                let codec = control >> 4;
                let _rate = (control >> 2) & 3;
                let size = (control >> 1) & 1;
                let channel = control & 1;

                match codec {
                    10 => {
                        let aac_packet_type = payload.get_u8();
                        match aac_packet_type {
                            0 => {
                                eprintln!("[AAC] AudioSpecificConfig");
                                eprintln!("\t{:02x?}", payload.chunk());

                                self.need_write_init_seg = true;

                                self.moov.traks[1].mdia.minf.stbl.stsd.entries.push(
                                    isobmff::moov::SampleEntry::mp4a {
                                        base: Box::new(isobmff::moov::SampleEntry::Audio {
                                            base: Box::new(isobmff::moov::SampleEntry::Base {
                                                handler_type: isobmff::types::types::mp4a,
                                                data_reference_index: 1,
                                            }),

                                            channel_count: match channel {
                                                0 => 1,
                                                1 => 2,
                                                _ => unreachable!(),
                                            },
                                            sample_size: match size {
                                                0 => 8,
                                                1 => 16,
                                                _ => unreachable!(),
                                            },
                                            sample_rate: self.samplerate << 15,
                                        }),
                                        ext: isobmff::Object {
                                            box_type: 0x65736473,
                                            payload: {
                                                let mut esds = isobmff::FullBox::new(0, 0).as_bytes();
                                                { // ES_Descriptor
                                                    esds.put_u8(0x03);
                                                    esds.put(&[0x80, 0x80, 0x80, 0x20 + payload.len() as u8][..]);
                                                    esds.put_u16(2);    // ES_ID
                                                    esds.put_u8(0x00);
                                                    { // DecoderConfigDescriptor
                                                        esds.put_u8(0x04);
                                                        esds.put(&[0x80, 0x80, 0x80, 0x12 + payload.len() as u8][..]);
                                                        esds.put_u8(0x40);  // Object Type Indicator: Audio ISO/IEC 14496-3
                                                        esds.put_u8(0x15);  // Stream Type: AudioStream
                                                        esds.put(&[0x00, 0x00, 0x00][..]);  // bufferSizeDB
                                                        esds.put_u32(4433); // maxBitrate
                                                        esds.put_u32(4433); // avgBitrate
                                                        { // DecoderSpecificInfo
                                                            esds.put_u8(0x05);
                                                            esds.put(&[0x80, 0x80, 0x80, payload.len() as u8][..]);
                                                            esds.put(payload.chunk());
                                                        }
                                                    }
                                                    { // SLConfigDescriptor
                                                        esds.put_u8(0x06);
                                                        esds.put(&[0x80, 0x80, 0x80, 0x01][..]);
                                                        esds.put_u8(0x02);
                                                    }
                                                }

                                                esds
                                            }
                                        }.as_bytes(),
                                    }
                                );
                            }
                            1 => {
                                self.trun_a.push(payload.len() as u32);
                                self.data_a.put(payload.chunk());

                                self.f_a.write_u16::<BigEndian>(0xfff1).unwrap();
                                let sampling_frequency_index = match self.samplerate {
                                    96000 => 0x0,
                                    88200 => 0x1,
                                    64000 => 0x2,
                                    48000 => 0x3,
                                    44100 => 0x4,
                                    32000 => 0x5,
                                    24000 => 0x6,
                                    22050 => 0x7,
                                    16000 => 0x8,
                                    12000 => 0x9,
                                    11025 => 0xa,
                                    8000 => 0xb,
                                    7350 => 0xc,
                                    _ => 0xf
                                };
                                self.f_a.write_u32::<BigEndian>({
                                    // profile
                                    let mut v = 0b01;
                                    // sampling_frequency_index
                                    v = (v << 4) | sampling_frequency_index as u32;
                                    // channel_configuration
                                    v = (v << 4) | (channel << 1) as u32;
                                    // aac_frame_length
                                    v = (v << 17) | payload.len() as u32;
                                    v = (v << 5) + 0xff;

                                    v
                                } as u32).unwrap();
                                self.f_a.write_u8(0xfc).unwrap();
                                self.f_a.write_all(payload.chunk()).unwrap();
                            }
                            _ => unreachable!()
                        }
                    }
                    _ => {
                        eprintln!("Audio codec [{:?}] is not supported", codec);
                        eprintln!("{:02x?}", payload.chunk());
                    }
                }
            }
            rtmp::message::Message::Video { dts: _dts, control, mut payload } => {
                let frame = control >> 4;
                let codec = control & 0xF;
                let (avc_packet_type, cts) = if 7 == codec {
                    let t = payload.get_u8();
                    let mut s = 0_i32;
                    if 1 == t {
                        for i in payload.split_to(3).iter() {
                            s = s << 8 | (*i as i32);
                        }
                    }
                    (t, s)
                } else {
                    (0xFF, 0)
                };

                match codec {
                    7 => match avc_packet_type {
                        0 => { // AVC sequence header
                            eprintln!("[AVC] avcC: AVCDecoderConfigurationRecord");
                            {
                                eprintln!("{:02?}", payload.chunk());
                            }

                            self.need_write_init_seg = true;

                            let width = (self.moov.traks[0].tkhd.width >> 16) as u16;
                            let height = (self.moov.traks[0].tkhd.height >> 16) as u16;
                            self.moov.traks[0].mdia.minf.stbl.stsd.entries.push(
                                isobmff::moov::SampleEntry::avc1 {
                                    base: Box::new(isobmff::moov::SampleEntry::Visual {
                                        base: Box::new(isobmff::moov::SampleEntry::Base {
                                            handler_type: isobmff::types::types::avc1,
                                            data_reference_index: 1,
                                        }),

                                        width,
                                        height,
                                        horiz_resolution: 0x00480000,
                                        vert_resolution: 0x00480000,
                                        frame_count: 1,
                                        compressor_name: "".to_owned(),
                                        depth: 24,
                                    }),
                                    ext: {
                                        let mut v = isobmff::Object {
                                            box_type: isobmff::types::types::avcC,
                                            payload: {
                                                let mut v = payload.clone();

                                                let _ = v.split_to(3);

                                                v
                                            },
                                        }.as_bytes();

                                        v.put(isobmff::Object {
                                            box_type: isobmff::types::types::colr,
                                            payload: {
                                                let mut colr = BytesMut::with_capacity(11);

                                                colr.put_u32(0x6e636c78);
                                                colr.put_u16(6);
                                                colr.put_u16(1);
                                                colr.put_u16(6);
                                                colr.put_u8(0);

                                                colr
                                            },
                                        }.as_bytes());

                                        v
                                    }
                                }
                            );

                            let _ = payload.split_to(9);
                            // sps
                            {
                                let len = payload.get_u16();
                                self.f_v.write_u32::<BigEndian>(1).unwrap();
                                self.f_v.write_all(payload.split_to(len as usize).chunk()).unwrap();
                            }
                            let _ = payload.split_to(1);
                            // pps
                            {
                                let len = payload.get_u16();
                                self.f_v.write_u32::<BigEndian>(1).unwrap();
                                self.f_v.write_all(payload.split_to(len as usize).chunk()).unwrap();
                            }
                        }
                        1 => { // AVC NALU
                            if 1 == frame {
                                self.flush_segment();
                            }

                            self.trun_v.push((payload.len() as u32, cts as u32));
                            self.data_v.put(payload.chunk());

                            while 0 < payload.len() {
                                let len = payload.get_u32();
                                self.f_v.write_u32::<BigEndian>(1).unwrap();
                                self.f_v.write_all(payload.split_to(len as usize).chunk()).unwrap();
                            }
                        }
                        2 => { // AVC end of sequence
                            // Empty
                        }
                        _ => unreachable!()
                    }
                    _ => {
                        eprintln!("Video codec [{:?}] is not supported", codec);
                        eprintln!("{:02x?}", payload.chunk());
                    }
                }
            }
            _ => {
                eprintln!("{:?}", msg)
            }
        }
    }

    fn write_init_seg(&mut self) {
        let mut f = File::create(format!("./{}/init.mp4", self.key)).unwrap();
        f.write_all(isobmff::Object {
            box_type: isobmff::ftyp::ftyp::BOX_TYPE,
            payload: isobmff::ftyp::ftyp {
                major_brand: isobmff::types::types::iso5,
                minor_version: 512,
                compatible_brands: vec![
                    isobmff::types::types::iso5,
                    isobmff::types::types::iso6,
                    isobmff::types::types::mp41,
                ],
            }.as_bytes(),
        }.as_bytes().chunk()).expect("Fail ftyp");
        f.write_all(isobmff::Object {
            box_type: isobmff::moov::moov::BOX_TYPE,
            payload: self.moov.as_bytes(),
        }.as_bytes().chunk()).expect("Fail moov");

        write!(self.f_playlist, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:{}\n#EXT-X-PLAYLIST-TYPE:EVENT\n#EXT-X-MAP:URI=\"init.mp4\"\n", self.sequence_number).unwrap();
    }

    fn flush_segment(&mut self) {
        if 0 == self.trun_v.len() {
            return;
        }
        if self.need_write_init_seg {
            self.need_write_init_seg = false;

            self.write_init_seg();
        }

        let sample_duration = self.moov.traks[0].mdia.mdhd.timescale/self.framerate;

        write!(self.f_playlist, "#EXTINF:{:0.3},\nseg_{}.m4s\n", (sample_duration * (self.trun_v.len() as u32)) as f32 / 1000., self.sequence_number).unwrap();

        let mut f = File::create(format!("./{}/seg_{}.m4s", self.key, self.sequence_number)).unwrap();

        self.sequence_number += 1;

        f.write_all(isobmff::Object {
            box_type: isobmff::moof::moof::BOX_TYPE,
            payload: {
                let mut moof = isobmff::moof::moof::default();

                moof.mfhd.sequence_number = self.sequence_number;

                moof.trafs.push({
                    let mut traf = isobmff::moof::traf::default();

                    traf.tfhd.track_id = 1;
                    traf.tfhd.default_sample_duration = Some(sample_duration);
                    traf.tfhd.default_sample_flags = Some(0x1010000);

                    traf.truns.push({
                        let mut trun = isobmff::moof::trun::default();

                        trun.first_sample_flags = Some(0x2000000);
                        let rate = self.moov.traks[0].mdia.mdhd.timescale as f32 / 1000.;
                        for (size, composition_time_offset) in self.trun_v.drain(..self.trun_v.len()) {
                            trun.samples.push((None, Some(size), None, Some((rate * composition_time_offset as f32) as u32)));
                        }

                        trun.data_offset = Some(0);

                        trun
                    });

                    traf
                });
                moof.trafs.push({
                    let mut traf = isobmff::moof::traf::default();

                    traf.tfhd.track_id = 2;
                    traf.tfhd.default_sample_duration = Some(1024);
                    traf.tfhd.default_sample_flags = Some(0x2000000);

                    traf.truns.push({
                        let mut trun = isobmff::moof::trun::default();

                        for size in self.trun_a.drain(..self.trun_a.len()) {
                            trun.samples.push((None, Some(size), None, None));
                        }

                        trun.data_offset = Some(0);

                        trun
                    });

                    traf
                });

                let data_offset = 16 + moof.len();
                moof.trafs[0].truns[0].data_offset = Some(data_offset as u32);
                moof.trafs[1].truns[0].data_offset = Some((data_offset + self.data_v.len()) as u32);

                moof
            }.as_bytes(),
        }.as_bytes().chunk()).expect("Fail on moof");

        f.write_all(isobmff::Object {
            box_type: isobmff::types::types::mdat,
            payload: {
                let mut v = self.data_v.split_to(self.data_v.len());
                v.put(self.data_a.split_to(self.data_a.len()));

                v
            },
        }.as_bytes().chunk()).expect("Fail on mdat");
    }
}

struct Connection {
    stream: TcpStream,
    ctx: rtmp::chunk::Chunk,

    prev_timestamp: Option<SystemTime>,
    prev_bytes_in: u32,
    bytes_out: u32,

    app: String,

    media_stream: Option<MediaStream>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            ctx: rtmp::chunk::Chunk::new(),

            prev_timestamp: None,
            prev_bytes_in: 0,
            bytes_out: 0,

            app: "".to_owned(),

            media_stream: None,
        }
    }

    pub fn start(&mut self) {
        self.handshaking();
        self.chunk_process();
    }

    pub fn handshaking(&mut self) {
        println!("Handshake Begin");

        let mut ctx = rtmp::handshake::Handshake::new();

        let mut buf = Vec::<u8>::with_capacity(1536);
        buf.insert(0, 0);    // 1 == buf.len(); for reading version
        loop {
            if let Ok(n) = self.stream.read(&mut buf) {
                unsafe { buf.set_len(n) }

                ctx.buffering(buf.as_slice());
            };

            match ctx.consume() {
                Ok(wr) => {
                    let _ = self.stream.write(wr.as_slice());
                },
                Err(rtmp::handshake::HandshakeError::Done) => break,
                Err(e) => {
                    eprintln!("Error while handshaking: {:?}", e);
                    return
                }
            }

            unsafe { buf.set_len(buf.capacity()) };
        }

        println!("Handshake Done");
    }

    fn flush(&mut self) {
        let len = self.ctx.get_write_buffer_length();
        if 0 < len {
            if None != self.prev_timestamp {
                self.bytes_out += len as u32;
            }

            self.stream.write_all(self.ctx.flush_write_buffer().chunk()).unwrap();
        }
    }

    fn chunk_process(&mut self) {
        let mut buf = vec!(0_u8, 128);
        loop {
            self.flush();
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    return
                }
                Ok(n) => {
                    unsafe { buf.set_len(n) }

                    self.ctx.buffering(buf.as_slice());
                }
                _ => {}
            }

            loop {
                match self.ctx.poll() {
                    Ok(None) => {
                        break
                    }
                    Ok(Some(rtmp::message::MessageStream{ stream_id, msg })) => {
                        if 0 == stream_id {
                            match msg {
                                rtmp::message::Message::SetChunkSize { chunk_size } => {
                                    if 1 + chunk_size as usize > buf.capacity() {
                                        buf.reserve_exact(1 + (chunk_size as usize) - buf.capacity())
                                    }
                                }
                                rtmp::message::Message::Command { payload } => {
                                    let cmd = {
                                        match &payload[0] {
                                            amf::Value::Amf0Value(amf::amf0::Value::String(str)) => str.as_str(),
                                            _ => {
                                                eprintln!("Unexpected {:?}", payload);
                                                return
                                            }
                                        }
                                    };
                                    let transaction_id = &payload[1];
                                    match cmd {
                                        "connect" =>  self.connect(payload),
                                        "_checkbw" =>  self._checkbw(payload),
                                        "releaseStream" => self.releaseStream(payload),
                                        "FCPublish" => self.FCPublish(payload),
                                        "createStream" => self.createStream(payload),
                                        "FCUnpublish" => self.FCUnpublish(payload),
                                        "deleteStream" => self.deleteStream(payload),
                                        _ => {
                                            eprintln!("{:?} {:?} {:?}", cmd, transaction_id, payload)
                                        }
                                    }
                                }
                                _ => eprintln!("{}: {:?}", stream_id, msg)
                            }
                        } else {
                            match msg {
                                rtmp::message::Message::Command { payload } => {
                                    let cmd = {
                                        match &payload[0] {
                                            amf::Value::Amf0Value(amf::amf0::Value::String(str)) => str.as_str(),
                                            _ => {
                                                eprintln!("Unexpected {:?}", payload);
                                                return
                                            }
                                        }
                                    };
                                    let transaction_id = &payload[1];
                                    match cmd {
                                        "publish" => self.publish(payload),
                                        _ => {
                                            eprintln!("{:?} {:?} {:?}", cmd, transaction_id, payload)
                                        }
                                    }
                                }
                                _ => if let Some(ref mut media_stream) = self.media_stream { media_stream.process(msg) }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error while chunk processing: {:?}", e);
                        return
                    }
                }
            }

            unsafe { buf.set_len(buf.capacity()) };
        }
    }

    // RPC methods
    #[allow(non_snake_case)]
    fn connect(&mut self, packet: amf::Array<amf::Value>) {
        self.ctx.push(2, rtmp::message::Message::WindowAckSize { ack_window_size: 2_500_000 });
        self.ctx.push(2, rtmp::message::Message::SetPeerBandwidth { ack_window_size: 10_000_000, limit_type: rtmp::chunk::LimitType::Dynamic });
        self.ctx.push(2, rtmp::message::Message::SetChunkSize { chunk_size: 256 });

        let transaction_id = &packet[1];
        if let amf::Value::Amf0Value(amf::amf0::Value::Object(obj)) = &packet[2] {
            self.app = if let amf::amf0::Value::String(str) = obj["app"].clone() {
                str
            } else { "".to_owned() };
            eprintln!("{:?}({:?})", "connect", obj["app"]);

            self.ctx.push(3, rtmp::message::Message::Command { payload: amf::Array::<amf::Value>::from([
                amf::Value::Amf0Value(amf::amf0::Value::String("_result".to_string())),
                transaction_id.clone(),
                amf::Value::Amf0Value(amf::amf0::Value::Object(amf::Object {
                    class_name: "".to_string(),
                    property: amf::Property::from([
                        ("fmsVer".to_string(), amf::amf0::Value::String("FMS/3,5,3,824".to_string())),
                        ("capabilities".to_string(), amf::amf0::Value::Number(127.)),
                        ("mode".to_string(), amf::amf0::Value::Number(1.)),
                    ])
                })),
                amf::Value::Amf0Value(amf::amf0::Value::Object(amf::Object {
                    class_name: "".to_string(),
                    property: amf::Property::from([
                        ("level".to_string(), amf::amf0::Value::String("status".to_string())),
                        ("code".to_string(), amf::amf0::Value::String("NetConnection.Connect.Success".to_string())),
                        ("description".to_string(), amf::amf0::Value::String("Connection succeeded.".to_string())),
                        ("objectEncoding".to_string(), amf::amf0::Value::Number(0.)),
                        ("data".to_string(), amf::amf0::Value::ECMAArray(amf::Property::from([
                            ("version".to_string(), amf::amf0::Value::String("3,5,3,824".to_string())),
                        ]))),
                    ])
                })),
            ]) });

            // Determine RTT and bandwidth by reply _checkbw message
            if None == self.prev_timestamp {
                self.flush();

                self.prev_timestamp = Some(SystemTime::now());
                self.prev_bytes_in = self.ctx.get_bytes_in();
                self.bytes_out = 0;
                self.ctx.push(3, rtmp::message::Message::Command {
                    payload: amf::Array::<amf::Value>::from([
                        amf::Value::Amf0Value(amf::amf0::Value::String("onBWDone".to_string())),
                        amf::Value::Amf0Value(amf::amf0::Value::Number(0.)),
                        amf::Value::Amf0Value(amf::amf0::Value::Null),
                    ])
                });

                self.flush();
            }
        }
    }

    #[allow(non_snake_case)]
    fn _checkbw(&mut self, _packet: amf::Array<amf::Value>) {
        if let Some(prev) = self.prev_timestamp {
            match prev.elapsed() {
                Ok(elapsed) => {
                    let secs = elapsed.as_secs_f64();
                    eprintln!("[Estimated BW] RTT: {:?}s In/Out: {:.4?}/{:.4?} KB/S", secs, (self.ctx.get_bytes_in() - self.prev_bytes_in) as f64 / 1024. / secs, self.bytes_out as f64 / 1024. / secs);
                }
                _ => {}
            }
            self.prev_timestamp = None;
        }
    }

    #[allow(non_snake_case)]
    fn releaseStream(&mut self, packet: amf::Array<amf::Value>) {
        if let amf::Value::Amf0Value(amf::amf0::Value::String(stream_key)) = &packet[3] {
            eprintln!("{:?}({:?})", "releaseStream", stream_key);
        }
    }

    #[allow(non_snake_case)]
    fn FCPublish(&mut self, packet: amf::Array<amf::Value>) {
        if let amf::Value::Amf0Value(amf::amf0::Value::String(stream_key)) = &packet[3] {
            eprintln!("{:?}({:?})", "FCPublish", stream_key);
        }
    }

    #[allow(non_snake_case)]
    fn createStream(&mut self, packet: amf::Array<amf::Value>) {
        let transaction_id = &packet[1];
        self.ctx.push(3, rtmp::message::Message::Command { payload: amf::Array::<amf::Value>::from([
            amf::Value::Amf0Value(amf::amf0::Value::String("_result".to_string())),
            transaction_id.clone(),
            amf::Value::Amf0Value(amf::amf0::Value::Null),
            amf::Value::Amf0Value(amf::amf0::Value::Number(1.)),
        ]) })
    }

    #[allow(non_snake_case)]
    fn publish(&mut self, packet: amf::Array<amf::Value>) {
        let name = {
            match &packet[3] {
                amf::Value::Amf0Value(amf::amf0::Value::String(str)) => str.as_str(),
                _ => {
                    eprintln!("Unexpected {:?}", packet);
                    return
                }
            }
        };
        let publish_type = {
            match &packet[4] {
                amf::Value::Amf0Value(amf::amf0::Value::String(str)) => str.as_str(),
                _ => {
                    eprintln!("Unexpected {:?}", packet);
                    return
                }
            }
        };

        self.media_stream = Some(MediaStream::new(name.to_owned()));
        eprintln!("{:?}({:?}, {:?})", "publish", name, publish_type);

        self.ctx.push(5, rtmp::message::Message::Command { payload: amf::Array::<amf::Value>::from([
            amf::Value::Amf0Value(amf::amf0::Value::String("onStatus".to_string())),
            amf::Value::Amf0Value(amf::amf0::Value::Number(0.)),
            amf::Value::Amf0Value(amf::amf0::Value::Null),
            amf::Value::Amf0Value(amf::amf0::Value::Object(amf::Object {
                class_name: "".to_string(),
                property: amf::Property::from([
                    ("level".to_string(), amf::amf0::Value::String("status".to_string())),
                    ("code".to_string(), amf::amf0::Value::String("NetStream.Publish.Start".to_string())),
                ])
            })),
        ]) })
    }

    #[allow(non_snake_case)]
    fn FCUnpublish(&mut self, packet: amf::Array<amf::Value>) {
        if let amf::Value::Amf0Value(amf::amf0::Value::String(stream_key)) = &packet[3] {
            eprintln!("{:?}({:?})", "FCUnpublish", stream_key);
        }
    }

    #[allow(non_snake_case)]
    fn deleteStream(&mut self, packet: amf::Array<amf::Value>) {
        if let amf::Value::Amf0Value(amf::amf0::Value::Number(stream_id)) = &packet[3] {
            eprintln!("{:?}({:?})", "deleteStream", stream_id);
        }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.1.2.7:1935")?;

    let pool = thread_pool::ThreadPool::new(4);

    for stream in listener.incoming() {
        if let Ok(s) = stream {
            pool.spawn(|| {
                Connection::new(s).start();
            });
        }
    }

    Ok(())
}
