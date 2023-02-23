from lstore.wide_page import Wide_Page

class Bufferpool:

    def __init__(self, max_pages: int):
        """
        self.{base, tail}_pages = {
            'index': {
                semaphore_count: int,
                dirty: bool,
                wide_page: Wide_Page 
            }
        }
        """
        self.base_pages = {}
        self.tail_pages = {}
        self.num_pages = 0
        self.max_pages = max_pages

    def write_page(self, index: int, base_page: bool) -> bool:
        """
        :param index: index to write
        :param base_page: bool to determine if base page or tail page
        :return: bool
        """
        obj = self.base_pages if base_page else self.tail_pages

        if index not in obj:
            return False

        dirty = obj[index]['dirty']
        wide_page = obj[index]['wide_page']

        if dirty:
            if not wide_page.write_to_disk(index, base_page):
                return False
            obj[index]['dirty'] = False
            return True

        # maybe return true even though nothing written?
        return False

    def retrieve_page(self, index: int, is_base_page: bool, num_columns: int) -> Wide_Page:
        """
        :param index: index to retrieve
        :param base_page: bool to determine if base page or tail page
        :return: Wide_Page
        """
        obj = self.base_pages if is_base_page else self.tail_pages

        if index in obj:
            return obj[index]['wide_page']

        if self.num_pages == self.max_pages:
            self.evict()

        wide_page = Wide_Page(num_columns, 0)
        wide_page.read_from_disk(index, is_base_page)
        obj[index] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }
        self.num_pages += 1
        return wide_page

    def mark_dirty(self, index: int, is_base_page: bool) -> bool:
        """
        :param index: index to mark dirty
        :param base_page: bool to determine if base page or tail page
        :return: bool
        """
        obj = self.base_pages if is_base_page else self.tail_pages

        if index not in obj:
            return False

        obj[index]['dirty'] = True
        return True

    def evict():
        """
        let's just go with lru
        If the page being evicted is dirty, then it must be written back to disk before
        discarding it. You may use any replacement policies of your choice, for example,
        Least-recently-used (LRU) or most-recently-used (MRU).

        if time, fifo, mru, etc. for graphs and stuff
        """
        pass
        
    def pin(self, index: int, base_page: bool) -> bool:
        """
        this should increase semaphore count,
        meaning that the page cannot be evicted
        """
        try:
            obj = self.base_pages if base_page else self.tail_pages
            obj[index].semaphore_count += 1
            return True
        except:
            return False

    def unpin(self, index: int, base_page: bool) -> bool:
        """
        this should decrease semaphore count,
        meaning that the page can be evicted
        """
        try:
            obj = self.base_pages if base_page else self.tail_pages
            if obj[index].semaphore_count > 0:
                obj[index].semaphore_count -= 1
            return True
        except:
            return False