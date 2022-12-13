use crate::page::Page;
use common::ids::PageId;
use common::{CrustyError, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock, Mutex};
use std::os::unix::fs::FileExt;

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization. 
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    // TODO milestone hs (add new fields)
    pub heap: Arc<RwLock<File>>,
    pub num_of_pages: Arc<Mutex<u16>>,
    //pub insertion: u8,

    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path and container Id. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        // TODO milestone hs

        Ok(HeapFile {
            // TODO milestone hs init your new field(s)
            //heap: Arc::RwLock::new(file),
            heap: Arc::new(RwLock::new(file)),
            num_of_pages: Arc::new(Mutex::new(0)),
            //required for correctness checks
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        return *self.num_of_pages.lock().unwrap();
        //panic!("TODO milestone hs");
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        //creating the buffer with nothing in it of length PAGE_SIZE
        let mut buffer = [0; PAGE_SIZE];

        //getting the correct bytes
        let f = self.heap.read().unwrap();

        //let beginning = (pid - 1) * PAGE_SIZE as u16;
        //let end =  pid * PAGE_SIZE as u16;

        //get an array of bytes to be used in 
        //dont want it to be inclusive because end will represent the offset of the next page,
        //so we want everything up until that point. 
        let offset = pid as u64 * PAGE_SIZE as u64;

        f.read_at(&mut buffer, offset.into())?;

        return Ok(Page::from_bytes(&buffer))
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        //calculate the offset of the page being added
        let offset = page.header.p_id as u64 * PAGE_SIZE as u64;

        println!("offset for insertion: {}", offset);

        //creating the buffer of bytes to be added to the file
        //let _page = my::Page::new(Page);
        //let buffer = Page::get_bytes(&_page);
        let mut buffer = page.get_bytes();

        //println!("buffer: {}", buffer);

        //prepping file
        //let mut f = self.heap.read().unwrap();
        let f = self.heap.read().unwrap();

        //adding the buffer to the correct offset
        f.write_at(&mut buffer, offset.into())?;
        //self.heap = f;

        //checking to see if we need to increase the number of pages
        let pid = page.header.p_id;
        let mut page_count = self.num_of_pages.lock().unwrap();
        
        println!("pagecount : {}, pageid : {}", *page_count, pid);

        if pid >= *page_count 
        {
            //self.num_of_pages  = page_count + 1;
            *page_count += 1;
            println!("made it into pid >= pagecount");
            //println!("page count: {}", self.num_of_pages.lock().unwrap());
        }

        return Ok(());
        //panic!("TODO milestone hs");
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf()).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.get_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.get_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.get_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
