import scrapy
from datetime import datetime
now = datetime.now()

class WikiSpider(scrapy.Spider):
    name = "paragraphs"
    start_urls = [
    'https://en.wikinews.org/w/index.php?title=Category:Politics_and_conflicts&from=A'
    ]

    def parse(self, response):
        
        # following all links to category pages
        # i.e https://en.wikinews.org/wiki/Category:Abortion
        for i,link in enumerate(response.css('li div.CategoryTreeSection a')):
            if i == 50:
                break
            yield response.follow(link,callback = self.parse2)

    def parse2(self, response):

        # if there are subcategories, crawl each subcategory 
        # recursively with the original parse call
        if response.css('div.CategoryTreeSection'):
            yield scrapy.Request(response.url,callback=self.parse)

        # following all links to pages
        # (this css selection needs to be ammended to not include the links
        # in subcategories)
        for link in response.css('div.mw-category a'):
            yield response.follow(link,callback = self.parse3)
    
    def parse3(self,response):


        # scrape the contents
        content = response.css('p')                 
        body = content.css('*::text').getall()        
        
        # categorise
        date = body[0]
        text = ''.join(body[1:])
        title = response.css('title::text').get()

        # extract tags
        tags = str(response.css('div.catlinks a::text').getall()[2:])

        # url
        url = response.url
        domain = 'wikinews.org'

        yield {
            '' : '',
            'id': hash(title),
            'domain': domain,
            'type'  : 'reliable',
            'url'   : url,
            'content'  : text,
            'scraped_at' : now,
            'updated_at' : now,
            'inserted_at' : now,
            'title' : title,
            'authors' : '',
            'keywords': '',
            'meta_keywords': tags,
            'meta_description': '',
            'tags'  : '',
            'summary' : '',
            'source' : 'wikinews'
        }