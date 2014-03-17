##
# This script scrapes ratings from Amazon.com.
# It takes "gamelist.csv" as an input (in the same directory as the script),
# and it crawls the entire set of ratings for each product listed in
# gamelist.csv. It does this multiple reviews at a time, and has a lag/wait time
# between going to the next page of max(0, 1 + random.gauss(0,0.5)).
#
# Amazon.com uses a 5-star ranking system.
##

#from bs4 import BeautifulSoup
import BeautifulSoup
import urllib2
import re
import os
import time
import random
import csv
import sys
from datetime import datetime
import mysql.connector


def question1():
# Define the games to scrape ratings on, as well as URL to use
    game_list = list(csv.reader(open("gamelist.csv", "r")))


    # Open the initial URL for the game's reviews and get the text
    review_url ="http://www.amazon.com/Call-Duty-Black-Ops-Playstation-3/product-reviews/B007XVTR5S/ref=sr_1_4_cm_cr_acr_txt?ie=UTF8&showViewpoints=1"

    #-------------------------------------------------------------------------
    # Scrape the ratings
    #-------------------------------------------------------------------------
    page_no = 1
    sum_total_reviews = 0

    more = True

    while (more):
        # Open the URL to get the review data
        request = urllib2.Request(review_url)
        try:
            page = urllib2.urlopen(request)
        except urllib2.URLError, e:
            if hasattr(e, 'reason'):
                print 'Failed to reach url'
                print 'Reason: ', e.reason
                sys.exit()
            elif hasattr(e, 'code'):
                if e.code == 404:
                    print 'Error: ', e.code
                    sys.exit()

        content = page.read()
        content = content.strip('\n')
        #content.strip('\n')
        soup = BeautifulSoup.BeautifulSoup(content)

        mylist = []
        for text in soup.findAll('div', {'style': 'margin-left:0.5em;'}):
            tempList = text.findAll(text=True, recursive=False)
            review = ''
            for element in tempList:
                if(element != '\n'):
                    review += element
            review.strip('\n')
            mylist.append(review)
        more = False
        for link in soup.findAll('a'):
            if re.match('Next', link.text) <> None:
                review_url = link.get('href')
                more = True
                break

        # Print some status info
        current_time = datetime.now().strftime('%I:%M:%S%p')

        for element in mylist:
            print element + '\n'
        wait_time = round(max(0, 1 + random.gauss(0, 0.5)), 2)
        time.sleep(wait_time)
        # Increment the page number
        page_no += 1


def question2():
    class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):

      def fetchone(self):
        row = self._fetch_row()
        if row:
          return dict(zip(self.column_names, self._row_to_python(row)))
        return None

    cnx = mysql.connector.connect(user='mfdixon', database='MSAN692db')
    query = "Select loanstats.State,Loan_ID " \
            "from loanstats Inner join " \
            "(select State,avg(Interest_Rate) " \
            "avgRate from loanstats group by 1) a " \
            "on a.State= loanstats.State and a.avgRate< loanstats.Interest_Rate " \
            "Inner join (select State, avg(ascii(credit_grade) + substr(credit_grade, 2, 1)*0.2) grade " \
            "from loanstats group by 1) b on b.State= loanstats.State and " \
            "b.grade> (ascii(loanstats.credit_grade) + substr(loanstats.credit_grade, 2, 1)*0.2) " \
            "where loanstats.Loan_duration=36 order by 1,2;"
    cursor = cnx.cursor(cursor_class=MySQLCursorDict, buffered=True)
    cursor.execute(query)
    for row in cursor:
       print row["Loan_ID"], row["State"]
    cursor.close()
    cnx.close()


def test():
   # test questions 1 and 2

   question2()
   question1()

if __name__ == '__main__':
    test()