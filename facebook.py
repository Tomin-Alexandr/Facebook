# -*- coding: utf-8 -*-

# lib import block
import pymysql
from time import sleep
import re
import dateparser
import threading
from itertools import repeat
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
import sys, os



# Сounters
Post_InsertCount = 0
Post_UpdateCount = 0
Comments_InsertCount = 0
Comments_UpdateCount = 0


# Chrome Driver options
options = webdriver.ChromeOptions()
# Use random  User - Agent
options.add_argument("--user-agent=%s" % UserAgent().random)
# Disable image download
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs)

# All xpath for russian language
XPATH = {
    'GetPostLink_FromGroup'     : "//div[contains(@aria-label, 'Лента новостей')]//a[contains(text(),'Комментарии:')]",
    'GetPostLink_FromChannel'   : "//div[contains(@class, 'userContentWrapper')]//a[contains(text(),'Комментарии:')]",
    'GetTextContent_FromPost'   : "//div[contains(@data-testid, 'post_message')]",
    'GetCommentsButton_FromPost': "//div[contains(@class, 'userContentWrapper')]//a[contains(text(), 'Комментарии')]",
    'GetEmojiCount_FromPost'    : "//div[contains(@class, 'userContentWrapper')]//a[contains(@data-testid, 'UFI2ReactionsCount/root')]/span[1]",
    'GetShareCount_FromPost'    : "//div[contains(@class, 'userContentWrapper')]//a[contains(@ajaxify, '/ajax/shares/view')]",
    'GetDatePublished_FromPost' : "//div[contains(@class, 'userContentWrapper')]//span[contains(@class, 'timestampContent')]",
    'old_to_newCommentsButton'  : "//a[contains(text(),'От старых к новым')]",
    'MostActualCommentsButton'  : "//a[contains(text(),'Самые актуальные')]",
    'ChooseDisplayAllComments'  : "//div[contains(text(),'Показывать все комментарии, включая потенциальный спам.')]/parent::*/parent::*/parent::*/parent::*",
    'OpenAllComments'           : "//form[contains(@class, 'commentable_item')]//span[contains(text(),'Показать')]/parent::*/parent::*",
    'OpenRequestComments'       : "//span[contains(text(),'ответ')]//span[contains(text(),'ответ')]/parent::*/parent::*",
    'GetAllCommentsBlock'       : "(//div[contains(@aria-label, 'Комментарий')] | //div[contains(@aria-label, 'Ответ на комментарий')])",
    'CheckLargeComment'         : "*//a[contains(text(),'Ещё')]"
}

"""
MySQL Database. posts table:
----------------------------
url             text        # Post url
author          text        # Post starter name
content         text        # Text content from post
date_published  datetime    # Post Date published 
emoji_count     int         # Post emoji count
comments_count  int         # Post comments count
share_count     int         # Post share count

---------------------------
"""
"""
MySQL Database. comments table:
----------------------------
id                  int         # Comment id
post_link           text        # Link to parrent post
author_profile_link text        # Link to comment author page
author_name         text        # Name of comment author 
text                text        # Comment text
time                datetime    # Comment date published   
emoji_count         int         # Comment emoji count
---------------------------
"""
SQLRequests = {
    'PostInsert'    : "INSERT INTO facebook.posts (url, author, content, date_published, emoji_count, comments_count, share_count) VALUES (%s,%s,%s,%s,%s,%s,%s)",
    'CommentInsert' : "INSERT INTO comments (post_link, author_profile_link, author_name, text, time, emoji_count) VALUES (%s,%s,%s,%s,%s,%s)",
    'PostUpdate'    : "UPDATE facebook.posts SET emoji_count = %s, comments_count = %s, share_count = %s WHERE url = %s",
    'CommentUpdate' : "UPDATE facebook.comments SET emoji_count = %s WHERE text = %s and time = %s"
}


# Main Function for data parse
def runWorker(links):
    DB_Option = {'user': 'user', 'password': 'pass', 'database': 'facebook', 'ip': 'localhost'}
    DB_Connect = pymysql.connect(DB_Option['ip'], DB_Option['user'], DB_Option['password'], DB_Option['database'], charset='utf8mb4')
    DB_Cursor = DB_Connect.cursor()

    for worker in links:
        page_url = worker [0]
        name = worker[3]

        WorkerDriver = webdriver.Chrome(options=options)

        # Go to page
        WorkerDriver.get(page_url)

        # Get posts links

        # For groups
        posts_links = WorkerDriver.find_elements(By.XPATH, XPATH['GetPostLink_FromGroup'])
        # For channels
        if len(posts_links) == 0:
            posts_links = WorkerDriver.find_elements(By.XPATH, XPATH['GetPostLink_FromChannel'])

        posts_link = []

        for i in posts_links:
            href = i.get_attribute('href')
            posts_link.append(href)

        for post_link in posts_link:
            """BLOCK OF POST PARSING"""
            WorkerDriver.get(post_link)

            SkipWindow(WorkerDriver)
            SkipWindow(WorkerDriver)

            # Get text content
            try:
                text_content = WorkerDriver.find_element(By.XPATH, XPATH['GetTextContent_FromPost']).text
            except:
                text_content = ''
                print('Text content is not find')

            # Find comments button
            comments = WorkerDriver.find_element(By.XPATH, XPATH['GetCommentsButton_FromPost'])
            # Get comments count
            comments_count = re.sub(r'[^0-9]', '', comments.text)

            # Get emoji count
            try:
                emoji_count = WorkerDriver.find_element(By.XPATH, XPATH['GetEmojiCount_FromPost']).text
                emoji_count = CheckEmojiCount(emoji_count)
            except:
                emoji_count = 0

            # Get share count
            try:
                share_count = re.sub(r'[^0-9]', '', WorkerDriver.find_element(By.XPATH, XPATH['GetShareCount_FromPost']).text)
            except:
                share_count = 0

            # Get date of published
            date_published = WorkerDriver.find_element(By.XPATH, XPATH['GetDatePublished_FromPost']).text
            date_published = dateparser.parse(date_published)


            # Stat update
            global Post_InsertCount,Post_UpdateCount

            # Check dublicate & update post attributs if post is in database
            DB_Cursor.execute('SELECT url from facebook.posts where url = %s', (post_link,))
            if DB_Cursor.rowcount > 0:
                DB_Cursor.execute(SQLRequests['PostUpdate'], (emoji_count,comments_count,share_count,post_link))
                Post_UpdateCount += 1

            else:
            # Insert post in database
                DB_Cursor.execute(SQLRequests['PostInsert'], (post_link, name, text_content, date_published, emoji_count, comments_count, share_count))
                Post_InsertCount += 1
            DB_Connect.commit()



            """BLOCK OF COMMENTS PARSER"""
            sleep(2)
            try:
                comments.click()
            except:
                print('Comment button error')
            sleep(2)

            # Switch hierarchy of comments on the 'From old to New'
            try:
                all_comments = WorkerDriver.find_element(By.XPATH, XPATH['old_to_newCommentsButton'])
                all_comments.click()
            except:
                try:
                    all_comments = WorkerDriver.find_element(By.XPATH, XPATH['MostActualCommentsButton'])
                    all_comments.click()
                except:
                   # Comments block is empty 
                   pass

            sleep(2)
            # Сhoose to display all comments
            try:
                all_comments = WorkerDriver.find_element(By.XPATH, XPATH['ChooseDisplayAllComments'])
                all_comments.click()
            except:
               pass

            # Open all comments
            # Open the comment thread while it is possible
            while True:
                try:
                    sleep(2)
                    WorkerDriver.execute_script("document.documentElement.scrollTop = document.documentElement.scrollHeight;")
                    dw = WorkerDriver.find_elements(By.XPATH, XPATH['OpenAllComments'])
                    for temp_click in dw:
                        temp_click.click()
                    if len(dw) == 0:
                        break
                except:
                    break

            # Open answers to comments
            all_comments = WorkerDriver.find_elements(By.XPATH, XPATH['OpenRequestComments'])
            for i in all_comments:
                try:
                    i.click()
                    sleep(0.5)
                except:
                    pass

            # Get all comments block
            comments_block = WorkerDriver.find_elements(By.XPATH, XPATH['GetAllCommentsBlock'])
            WorkerDriver.execute_script("document.documentElement.scrollTop = document.documentElement.scrollHeight;")

            for comment in comments_block:
                # Check comment, if comment is large --> press 'more' button
                try:
                    comment.find_element(By.XPATH, XPATH['CheckLargeComment']).click()
                except:
                    pass
                # Transfot Selenium object to BS4 object
                soup = BeautifulSoup(comment.get_attribute('innerHTML'), 'html.parser')
                # Get page link of comment author
                author_profile_link = soup.select_one("*[class*='6qw4']").get('href')
                # Get nickname of comment author
                author_name = soup.select_one("*[class*='6qw4']").get_text()
                # Get text of comment
                if soup.select_one("span[dir*=ltr]") != None:
                    text = soup.select_one("span[dir*=ltr]").get_text()
                # Get time of published
                time = soup.select_one("*[class*='livetimestamp']").get('data-tooltip-content')
                try:
                    time = dateparser.parse(time)
                except Exception as E:
                    print('Unsupport date format')
                # Get emoji count
                if soup.select_one("*[class*='1lld']") != None:
                    comment_emoji_count = soup.select_one("*[class*='1lld']").get_text()
                else:
                    comment_emoji_count = 0

                # Stat update
                global Comments_InsertCount,Comments_UpdateCount
                
                # Check dublicate comment & update comment emoji count
                DB_Cursor.execute('SELECT time from facebook.comments where text = %s and time = %s', (text,time))
                if DB_Cursor.rowcount > 0:
                    DB_Cursor.execute(SQLRequests['CommentUpdate'],(comment_emoji_count,text,time))
                    Comments_UpdateCount += 1
                else:
                # Insert comment in DB
                    DB_Cursor.execute(SQLRequests['CommentInsert'], (post_link, author_profile_link, author_name, text, time, comment_emoji_count))
                    Comments_InsertCount += 1
                DB_Connect.commit()
            # Console info update
            os.system('cls||clear')
            print("New post's:      {} New comment's: {}".format(Post_InsertCount, Comments_InsertCount))
            print("Updated post's: {} Updated comment's: {}".format(Post_UpdateCount, Comments_UpdateCount))
        WorkerDriver.quit()



# Skip PopUp window
def SkipWindow(WorkerDriver):
    # Move to end of page
    WorkerDriver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(2)
    try:
        close_button = WorkerDriver.find_element(By.ID, 'expanding_cta_close_button')
        close_button.click()
    except:
        pass

# If emoji > 1000
def CheckEmojiCount(emoji_count):

    if 'тыс.' in str(emoji_count):
        emoji_count = re.sub(' тыс.', '', emoji_count)
        emoji_count = re.sub(',', '.', emoji_count)
        emoji_count = int(float(emoji_count) * 1000.0)
        return emoji_count
    else:
        return emoji_count

# Split list for Thread's
def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [alist[i*length // wanted_parts: (i+1)*length // wanted_parts] for i in range(wanted_parts)]


"""
MySQL Database. blogs table:
----------------------------
url	                text    # Link of channel or group page
name	            text    # Name of channel or group
subsribers	        int     # Subscriber count
name_in_facebook	text    # Full channel or group name
---------------------------
"""
DB_Option = {'user': 'user', 'password': 'pass', 'database': 'facebook', 'ip': 'localhost'}
DB_Connect = pymysql.connect(DB_Option['ip'], DB_Option['user'], DB_Option['password'], DB_Option['database'], charset='utf8mb4')
DB_Cursor = DB_Connect.cursor()
DB_Cursor.execute("SELECT * FROM blogs")
links = DB_Cursor.fetchall()

# Run thread's
threads = []

ThreadCount = 6 # 6 Thread's
for i in split_list(links,ThreadCount):
    t = threading.Thread(target=runWorker, args=(i,))
    threads.append(t)
    t.start()
