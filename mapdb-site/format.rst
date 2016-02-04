
StoreDirect
------------------

StoreDirect updates records on place. Space used by records get reused.
It keeps track of free space released deletion and reuses it.
It has zero protection from crash, all updates are written directly into store.

StoreDirect allocates space in 'pages' of size 1MB. Operations such as ``readLong``, ``readByte[]``
must be aligned so they do not cross page boundaries.


Index page
~~~~~~~~~~~~~~~~~~~~~~~~
Linked list of pages. It starts at ``FIRST_INDEX_PAGE_POINTER_OFFSET`` parity16.
Link to next page is at start of each page.

Last index page may not be completely filled. Maximal fill is indicated by Max Recid stored at ``INDEX_TAIL_OFFSET`` parity3+shift.

Index Page at start contains:

- first value is **pointer to next index page** parity16
- TODO second value in page is **checksum of all values** on page

Rest of the index page is filled with index values.


Sorted Table Map
---------------------

``SortedTableMap`` uses its own file format. File is split into page,
where page size is power of two and maximal page size 1MB.


