use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::path::{PathBuf, Path};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};


/// This is the basic data structure a container that maps a value ID to bytes
type ContainerMap = Arc<RwLock<HashMap<PageId, Vec<u8>>>>;


/// The StorageManager struct
/// A map for storing containers
#[derive(Serialize, Deserialize)]
pub struct StorageManager 
{
    //containers: Arc<RwLock<HashMap<ContainerId, ContainerMap>>>,
    #[serde(skip)]
    containers: Arc<RwLock<HashMap<ContainerId, HeapFile>>>,
    /// Path to database metadata files.
    pub storage_path: String,
    is_temp: bool,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> 
    {
        let containers = self.containers.read().unwrap();

        match containers.get(&container_id) 
        {
            Some (heapfile) => return Some(HeapFile::read_page_from_file(heapfile, page_id).unwrap()),
            None => return None
        }

        // if containers.contains_key(&container_id)
        // {
        //     let heap = containers.get(&container_id);
        //     let page = HeapFile::read_page_from_file(&heap, page_id);
        //     return page;
        // }
        // else
        // {
        //     return None;
        // }

        //panic!("TODO milestone hs");
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> 
    {
        //consider changing this to a match statement
        let containers = self.containers.read().unwrap();
        if containers.contains_key(&container_id)
        {
            let heap = containers.get(&container_id).unwrap();
            //let number_of_pages = heap.num_of_pages;
            HeapFile::write_page_to_file(heap, page);
            //Some(heapfile) => return Some(HeapFile::write_page_to_file(heapfile, Page).unwrap()),
            //Some(heapfile) => HeapFile::write_page_to_file(heapfile, page).unwrap()
            return Ok(());
        }
        else
        {
            return Err(CrustyError::CrustyError(format!("Container does not exist")));
        }
        //return Some(HeapFile::write_page_to_file(containers, page: Page));
        //panic!("TODO milestone hs");
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId 
    {
        let containers = self.containers.read().unwrap();
        let heap = containers.get(&container_id).unwrap();
        let number_of_pages = heap.num_of_pages.lock().unwrap();
        return *number_of_pages as PageId;
        // if containers.contains_key(&container_id)
        // {
        //     let heap = containers.get(&container_id).unwrap();
        //     let number_of_pages = heap.num_of_pages.lock().unwrap();
        //     return *number_of_pages as PageId;
        // }
        // else
        // {
        //     return Err(CrustyError::CrustyError(format!("Container does not exist")));
        // }
        //panic!("TODO milestone hs");
    }


    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) 
    {
        let containers = self.containers.read().unwrap();

        // if containers.contains_key(&container_id)
        // {
        //     let heap = containers.get(&container_id);
        //     return ((heap.read_count).into_inner, (heap.write_count).into_inner());
        // }
        // else
        // {
        //     return (0, 0);
        // }

        match containers.get(&container_id)
        {
            //Some(heapfile) => return((heapfile.read_count).into_inner().clone(), (heapfile.write_count).into_inner().clone()),
            //how to read a value from the atomic16
            //check how to do this in rust
            //must more direct way to do this
            Some(heapfile) => return(heapfile.read_count.load(Ordering::Relaxed), heapfile.write_count.load(Ordering::Relaxed)),
            None => return (0, 0),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self 
    {
        //#[serde(skip)]

        if Path::new(&storage_path).exists()
        {
            let sm = StorageManager
            {
            containers: Arc::new(RwLock::new(HashMap::new())),
            /// Path to database metadata files.
            storage_path: storage_path,
            is_temp: false,
            };
            return sm;
        }
        else
        {
            fs::create_dir_all(storage_path.clone());
            let sm = StorageManager
            {
            containers: Arc::new(RwLock::new(HashMap::new())),
            /// Path to database metadata files.
            storage_path: storage_path,
            is_temp: false,
            };
            return sm;
        }
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        debug!("Making new temp storage_manager {}", storage_path);
        let sm = StorageManager
        {
            containers: Arc::new(RwLock::new(HashMap::new())),
            /// Path to database metadata files.
            storage_path: storage_path,
            is_temp: true,
        };

        return sm;
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId 
    {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        panic!("TODO")
        
        // let containers = self.containers.read().unwrap();

        // if containers.contains_key(&container_id)
        // {
        //     let mut heapfile = containers.get(&container_id).unwrap();
        //     let mut page_id = 0;
        //     let mut num_pages = heapfile.num_of_pages.lock().unwrap();
        //     while page_id < num_pages
        //     {
        //         //HeapFile::read_page_from_file(heapfile, page_id).unwrap()
        //         let mut page = HeapFile::read_page_from_file(heapfile, page_id).unwrap();
        //         let mut slot_id_add = page.add_value(&value).unwrap();
        //         //HeapFile::read_page_from_file(heapfile, page_id).unwrap()
        //         HeapFile::write_page_to_file(heapfile, page);
        //         if slot_id_add.is_none()
        //         {
        //             page_id += 1;
        //         }
        //         else
        //         {
        //             return slot_id_add;
        //         }
        //     }

        //     let mut page = Page::new(num_of_pages - 1);
        //     let slot_id = page.add_value(&value);
        //     num_of_pages += 1;
        //     HeapFile::write_page_to_file(heapfile, page);
        //     return slot_id;
        // }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        panic!("TODO milestone hs");
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        panic!("TODO milestone hs");
    }

    /// Create a new container to be stored. 
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize 
    /// the container_config, name, container_type, or dependencies
    /// 
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> 
    {
        let mut containers = self.containers.write().unwrap();

        if containers.contains_key(&container_id)
        {
            return Err(CrustyError::IOError("ContainerId key already exists, you fool!".to_string()));
        }
        else
        {
            //get the storage path to add the new container to
            let pathbuf = PathBuf::from(&self.storage_path);
            //create a heapfile from the given path
            let heap = HeapFile::new(pathbuf);
            //add the container id as the key and the new heap as value
            containers.insert(container_id, heap.unwrap());
            //return OK(())
            return Ok(());
        }
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {

        if Path::new(&self.storage_path).exists()
        {
            
        }
        //call iterator on each page
        // let containers = self.containers.read().unwrap();
        // let heapfile = containers.get(&container_id).unwrap();
        // let page_count = heapfile.num_pages();

        // let mut i = 0;

        // while i < page_count
        panic!("arg");
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        panic!("TODO milestone hs");
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    fn clear_cache(&self) {
        panic!("TODO milestone hs");
    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    /// If not a temp SM, this should serialize the mapping between containerID and Heapfile. 
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        panic!("TODO milestone hs");
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.get_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError("Could not read row from CSV".to_string()))
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
/// bro i dont know if this works but i feel like my brain is fully shutting down.
/// future abby, if you are debugging this, you got this. sorry for doing you so dirty and ignoring this functionality
impl Drop for StorageManager {
    /// Shutdown the storage manager. Can call be called by shutdown. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn drop(&mut self) 
    {
        if self.is_temp
        {
            fs::remove_dir_all(&self.storage_path.clone());
        }
        //panic!("TODO milestone hs");
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
