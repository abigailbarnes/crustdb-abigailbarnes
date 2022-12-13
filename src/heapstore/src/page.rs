use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use std::convert::TryInto;
use std::mem;


/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values.

pub(crate) struct Page {
    /// The data for data
    pub data: [u8; PAGE_SIZE],
    pub header: GeneralMetadataHeader,
    pub metadata: Vec<Metadata>,
}

//currently occupying 6 bytes of 8 total available bytes
pub struct GeneralMetadataHeader
{
    pub p_id: PageId, //u16, 2bytes which represents over 65000 possible values
    pub position_last_record: u16, //2 bytes, position of the beginning of the most recently added record in bytes
    pub n_deletes: u16, //number of deleted records currently, length of free spaces
    pub n_records: u16, //length of the vector of metadata
    pub free_spaces_from_deleted_record: Vec<u16>, //2byte, keeps track of the indeces for the deleted records
}

pub struct Metadata
{
    pub record_size: u16,
    pub offset_value: u16, // represents, in bytes, where the "beginning" of a record is
    //note: an offset value of PAGE_SIZE - 1 indicates that there are no records in the page
}

/// The functions required for page
impl Page {
    /// Create a new page
    pub fn new(page_id: PageId) -> Self 
    {
        let head = GeneralMetadataHeader
        {
            p_id: page_id,
            position_last_record: PAGE_SIZE as u16 - 1, //adding to the last "byte" in the list of bytes in the page
            n_deletes: 0,
            n_records: 0,
            free_spaces_from_deleted_record: Vec::new(),
        };

        let new_page = Page
        {
            data: [0;PAGE_SIZE],
            header: head,
            metadata: Vec::new(),
        };
        //println!("{:?}", new_page)
        return new_page;
    
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId 
    {
        return self.header.p_id;
    }


    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should replace the slotId on the next insert.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    
    pub fn overwrite(&mut self, bytes: &[u8]) -> Option<SlotId>
    //function to overwrite if there is at least one unused SlotId from deleting a record
    {
        //if all slotids are currently in use
        if self.header.free_spaces_from_deleted_record.len() == 0
        {
            return None;
        }
        //if there is a free slotid to use
        else
        {
            //loop through the slotids
            for i in &self.header.free_spaces_from_deleted_record
            {   
                let i: usize = *i as usize;
                println!("{}", i);
                let old_metadata = &self.metadata[i];
                let index = 0;
                //check to see if there is enough free space in the given/current slotid
                if old_metadata.record_size >= bytes.len() as u16
                //proceed to reassign the metadata header
                {
                    let start_index = old_metadata.offset_value;
                    let end_index = start_index + bytes.len() as u16 - 1;

                    let remove_bytes = old_metadata.offset_value - bytes.len() as u16; 
                    //create a new metadata object for mapping
                    let new_metadata = Metadata 
                    {
                        record_size: bytes.len() as u16,
                        offset_value: old_metadata.offset_value,
                    };
                    //set metadata at index of the given slot id = new_metadata
                    self.metadata[i] = new_metadata;
                    //overwrite the data in the data array
                    self.data[start_index as usize..=end_index as usize].clone_from_slice(&bytes);
                    //remove slot_id from the "free space from deleted records" array
                    self.header.free_spaces_from_deleted_record.remove(index); // I THINK THIS MIGHT CAUSE MY CODE TO NOT WORK
                    //create slot id to prepare for return
                    let slot_id: SlotId = i as u16;
                    //reduce number of free spaces indicated by n_deleted
                    self.header.n_deletes = self.header.n_deletes - 1;
                    //return new slot id
                    return Some(slot_id);
                }
            }

            //if we make it out of the previous loop, this means that there was no available space to insert the new data into
            //the previously existing spaces... must add new space for it in data array, but update the same slot_id association
            //aka mapping in the metadata

            //acting as if we are adding "normally" to the data array
            //the only thing that will be different will be the metadata that will be mapped to it
            let end_index: u16 = self.header.position_last_record;
            let start_index: u16 = (end_index - bytes.len() as u16) + 1;

            self.data[start_index as usize..=end_index as usize].clone_from_slice(&bytes);

            let replacing_slot_id = self.header.free_spaces_from_deleted_record[0 as usize];

            let new_metadata = Metadata
            {
                record_size: bytes.len() as u16,
                offset_value: start_index,
            };

            //placing the new metadata object where the old one used to be
            self.metadata[replacing_slot_id as usize] = new_metadata;
            //removing the slot_id from the free space record
            let slot_id: SlotId = self.header.free_spaces_from_deleted_record[0 as usize];
            self.header.free_spaces_from_deleted_record.remove(0);
            //reduce number of free spaces indicated by n_deleted
            self.header.n_deletes = self.header.n_deletes - 1;
            return Some(slot_id);
        }
    }

    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> 
    {
        //position_last_record should be the start byte of the most recently added record
        //^check for that edge case
        //println!("byte length: {}", bytes.len());
        //let end_index: u16 = self.header.position_last_record;
        //println!("ending index: {}", end_index);
        //let start_index: u16 = (end_index - bytes.len() as u16) + 1;
        //println!("beginning index: {}", start_index);

        //checking to see if there if overwrite does anything
        let overwrite_output = self.overwrite(bytes);

        if overwrite_output == None
        //if there are no free spots from deletion, do things normally
        {
            //make sure there is enough free space on the page
            if self.get_largest_free_contiguous_space() > bytes.len()
            {
                let end_index: u16 = self.header.position_last_record;
                let start_index: u16 = (end_index - bytes.len() as u16) + 1;
                //if the position of the last record is at the end of the page aka no records in the page
                //if end_index == (PAGE_SIZE as u16 - 1)
                //{
                    //set the start index equal to the position of the most recently added record - number of bytes + 1
                    //= indicates inclusive
                    self.data[start_index as usize..=end_index as usize].clone_from_slice(&bytes);
                    
                    //creating new metadata to add to the metadata vector
                    let added_metadata = Metadata
                    {
                        record_size: bytes.len() as u16,
                        offset_value: start_index,
                    };

                    //adding to the metadata vector
                    self.metadata.push(added_metadata);

                    //changing the position of the most recently added record
                    self.header.position_last_record = start_index - 1;

                    let slot_id: SlotId = self.metadata.len() as u16 - 1;

                    //increasing the n_records value
                    self.header.n_records = self.header.n_records + 1;

                    return Some(slot_id);
                //}
                //for when you are not just adding to a blank page!
            }
            else
            {
                //return -1; // CHANGED FROM NONE
                return None;
            }
        }
        else
        //there are free spots from deletion, so just return the output of overwrite
        {
            return overwrite_output;
        }
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> 
    {

        if slot_id > self.metadata.len() as u16 - 1
        {
            return None;
        }
        else if self.header.free_spaces_from_deleted_record.contains(&slot_id)
        {
            return None;
        }
        else
        {
            let single_metadata = &self.metadata[slot_id as usize];
            let start_index = single_metadata.offset_value as usize;
            let end_index = single_metadata.record_size as usize + start_index;
            //println!("start index: {}", start_index);
            //println!("end index: {}", end_index);
            let bytes = &self.data[start_index..end_index];
            return Some(bytes.to_vec());
        }
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        //panic!("TODO milestone pg");
        if slot_id > self.metadata.len() as u16 - 1
        {
            return None;
        }
        else
        {
            self.header.free_spaces_from_deleted_record.push(slot_id);
            self.header.n_deletes = self.header.n_deletes + 1;
            return Some(());
        }
    }

    /// Create a new page from the byte array.
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self 
    {
        let mut data_to_add: [u8;PAGE_SIZE] = [0;PAGE_SIZE]; // check to see if i can do this?
        data_to_add.copy_from_slice(&data);
        let p_id_to_add: u16 = u16::from_le_bytes(data[0..=1].try_into().unwrap());
        let position_last_record_to_add: u16 = u16::from_le_bytes(data[2..=3].try_into().unwrap());
        let n_deletes_to_add: u16 = u16::from_le_bytes(data[4..=5].try_into().unwrap());
        let n_records_to_add: u16 = u16::from_le_bytes(data[6..=7].try_into().unwrap());

        let mut index = 8;
        //creating the free spaces vector
        let mut free_spaces_from_deleted_record_to_add: Vec<u16> = Vec::new();
        let mut i = 0;
        while i < n_deletes_to_add
        {
            free_spaces_from_deleted_record_to_add. push(u16::from_le_bytes(data[index..=(index + 1)].try_into().unwrap()));
            index = index + 2;
            i = i + 1;
        }

        //creating the metadata vector
        let mut metadata_to_add: Vec<Metadata> = Vec::new();
        i = 0;
        while i < n_records_to_add
        {
            //create a new metadata object
            let record_size_to_add = u16::from_le_bytes(data[index..=(index + 1)].try_into().unwrap());
            index = index + 2;
            let offset_value_to_add = u16::from_le_bytes(data[index..=(index + 1)].try_into().unwrap());
            index = index + 2;
            let new_metadata = Metadata
            {
                record_size: record_size_to_add,
                offset_value: offset_value_to_add,
            };

            metadata_to_add.push(new_metadata);
            i = i + 1;
        }

        //all of this information comes together to create the header! yay!
        //one day, all i hope is that bytes become more intuitive because, dear lord,
        //this took me so long to think about
        //okay so now i need to like fr create the page
        //i need to remember to delete these comments
        //DELETE THESE >:(

        //meep meep moop moop clone the data into the array! <3

        let header_to_add = GeneralMetadataHeader
        {
            p_id: p_id_to_add,
            position_last_record: position_last_record_to_add,
            n_deletes: n_deletes_to_add,
            n_records: n_records_to_add,
            free_spaces_from_deleted_record: free_spaces_from_deleted_record_to_add,
        };

        let page = Page
        {
            data: data_to_add,
            header: header_to_add,
            metadata: metadata_to_add,
        };

        return page;

    }

    /// Convert a page into bytes. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    pub fn get_bytes(&self) -> Vec<u8> 
    {
        let mut byte_vector: Vec<u8> = Vec::new();

        let mut p_id_bytes_to_add = self.header.p_id.to_le_bytes().to_vec();
        byte_vector.append(&mut p_id_bytes_to_add);
        //println!("p_id bytes: {}", byte_vector.len());

        let mut position_last_record_to_add = self.header.position_last_record.to_le_bytes().to_vec();
        byte_vector.append(&mut position_last_record_to_add);
        //println!("position last record bytes: {}", byte_vector.len());

        let mut n_deletes_to_add = self.header.n_deletes.to_le_bytes().to_vec();
        byte_vector.append(&mut n_deletes_to_add);
        //println!("n_deletes bytes: {}", byte_vector.len());

        let mut n_records_to_add = self.header.n_records.to_le_bytes().to_vec();
        byte_vector.append(&mut n_records_to_add);
        //println!("n_records bytes: {}", byte_vector.len());

        let mut index = 0;

        while index < self.header.n_deletes
        {
            //println!("number of free spaces: {}", self.header.n_deletes);
            let mut single_free_record_to_add = self.header.free_spaces_from_deleted_record[index as usize].to_le_bytes().to_vec();
            byte_vector.append(&mut single_free_record_to_add);
            index = index + 1;
            //println!("free space bytes: {}", byte_vector.len());
        }

        index = 0;

        while index < self.header.n_records
        {
            //println!("number of metadata: {}", self.header.n_records);
            let mut record_size_to_add = self.metadata[index as usize].record_size.to_le_bytes().to_vec();
            byte_vector.append(&mut record_size_to_add);
            let mut offset_value_to_add = self.metadata[index as usize].offset_value.to_le_bytes().to_vec();
            byte_vector.append(&mut offset_value_to_add);
            index = index + 1;
            //println!("metadata bytes: {}", byte_vector.len());
        }

        let bytes_of_header = self.get_header_size();
        let mut data_to_add = self.data[bytes_of_header..PAGE_SIZE].to_vec();
        byte_vector.append(&mut data_to_add);
        //println!("get bytes: {}", byte_vector.len());
        return byte_vector;
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        //panic!("TODO milestone pg");
        let number_deleted_records = self.header.free_spaces_from_deleted_record.len();
        let bytes_deleted_records = number_deleted_records * 2; //2 bytes for each deleted record
        let bytes_metadata = self.metadata.len() * 4;
        let bytes_original_header = 8;
        //let header_bytes = mem::size_of_val(&self.header);
        //let metadata_vector_bytes = mem::size_of_val(&self.metadata.len()) * 4;
        //println!("headersize: {}", header_bytes + metadata_vector_bytes);
        //return header_bytes + metadata_vector_bytes;
        return bytes_deleted_records + bytes_metadata + bytes_original_header;


    }

    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        //panic!("TODO milestone pg");
        return self.header.position_last_record as usize - self.get_header_size() + 1;
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
pub struct PageIter 
{
    page: Page,
    page_index:usize,
}

/// The implementation of the (consuming) page iterator.
impl Iterator for PageIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {

        let mut index = self.page_index;

        //panic!("TODO milestone pg");
        //check to make sure the index is within the valid bounds of the page
        if self.page.header.n_records <= index as u16
        {
            return None;
        }
        //if there are no deleted spaces
        else if self.page.header.n_deletes == 0
        {
            self.page_index = self.page_index + 1;

            let begin = self.page.metadata[index].offset_value as usize;
	        let end = begin + self.page.metadata[index].record_size as usize;

	        let size_of_record = self.page.metadata[index].record_size as usize;
	        
	        return Some(self.page.data[begin..end].to_vec());
        }
        //if there are currently deleted records to be skipped over
        else
        {
            let mut i = 0;
            while i < self.page.header.n_deletes
            {
                //if we hit a slot that has been deleted, skip it and set i back to 0!
                if index == self.page.header.free_spaces_from_deleted_record[i as usize] as usize
                {
                    i = 0;
                    index = index + 1;
                }
                else if index >= self.page.metadata.len()
                {
                    return None;
                }
                else 
                {
                    i = i + 1;
                }            
            }

            self.page_index = index + 1;

	        let size = self.page.metadata[index].record_size as usize;
	        let begin = self.page.metadata[index].offset_value as usize;
	        let end = begin + size;
	        return Some(self.page.data[begin..end].to_vec());

        }
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page 
{
    type Item = Vec<u8>;
    type IntoIter = PageIter;

    fn into_iter(self) -> Self::IntoIter 
    {
        PageIter
        {
            page: self,
            page_index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let mut p = Page::new(0);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}
