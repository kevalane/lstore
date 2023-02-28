from lstore.wide_page import Wide_Page

class Page_Range:
    def __init__(self):
        self.base_pages = []
        self.tail_pages = []
        self.capacity = 18

    def has_capacity(self):
        return len(self.base_pages) < self.capacity

    def add_base(self, base_page):
        self.base_pages.append(base_page)

    def add_tail(self, tail_page):
        self.tail_pages.append(tail_page)