#IMDB crawler
import requests
import nltk
from nltk import RegexpParser
from nltk.corpus import treebank
from nltk.corpus import sentiwordnet as swn
from lxml import html
from bs4 import BeautifulSoup
from time import sleep 
import sqlite3
#遇到亂碼可以用join函數存取
con = sqlite3.connect('imdb.sqlite')
sql = 'insert into movie_comments_rating(film_name,author,score) values(?,?,?)'
cur = con.cursor()



_URL = requests.get('http://www.imdb.com/chart/top?sort=us,desc&mode=simple&page=1')
tree = html.fromstring(_URL.text)
linkURL = tree.xpath("//tbody[@class='lister-list']//tr//td[@class='titleColumn']/a/@href")
headtitle = tree.xpath("//tbody[@class='lister-list']//tr//td[@class='titleColumn']/a/text()") #250部電影
yearForEachMovie = tree.xpath("//div[@class = 'lister'] // tbody[@class = 'lister-list']//tr//td[@class = 'titleColumn']//span//text()")
#儲存超連結網址，內容為要進入每個排名電影的超連結
URL_List = []



#用來拼接到每個網頁網址的list
URL_To_EachMovie = []
#儲存每個評論
Each_Comments = []
#儲存每個評論給的分數
Each_Rate = []
#儲存每個圖片的網址
_Score = []

#存進資料庫
listscore = []
headtitle_list = []
comment_author_list = []
#存進資料庫


# 抓...前20個排名高的電影(最近)
cnt = 1
for i in linkURL:
    page = 1 #記錄換頁 IMDB裡評論的下一頁是前一頁*10
    URL_List.append(i)
    #print(URL_List)
    temp = URL_List.pop()
    
    #做拼接而且進入該網頁，因為進入每個評論的attributes都是同一個 "reviews?ref_=tt_ov_rt" 所以直接用
    URL_Link_To_Comments_array = temp.split('/?')  #結果 /title/tt2119532 
    URL_Link_To_Comments = URL_Link_To_Comments_array.pop(0)
    #進入評論的網頁
    Link_To_Comments = requests.get('http://www.imdb.com' + URL_Link_To_Comments + "/reviews?ref_=tt_ov_rt")  
    tree2 = html.fromstring(Link_To_Comments.text)
    #print(tree2.text)
    linkURL2 = tree2.xpath("//div[@id = 'tn15content']//p/text()")
    #linkURL2 = tree2.xpath('//*[@id="tn15content"]//p/text()')
    #join
    # str.join(iterable)	回傳將 str 連結 iterable 各元素的字串
    status = True #判斷有沒有到最後一頁
    while(status == True):        
        tree3 = html.fromstring(Link_To_Comments.text)
        author = tree3.xpath("//div[@id = 'tn15content'] // div // a[2] // text()") #print 10筆作者資料 
        for j in range(0,len(author)):
            comment_author_list.append(author.pop(0)) #儲存作者名字  資料庫~~~~~~~~~~~~~~
        #print(comment_author_list)          
    
    
    #---------------------------------評分處理-------------------------------------------            
        soup = BeautifulSoup(Link_To_Comments.text)    
        for j in soup.findAll('div' , id = 'tn15content'):        
            for k in j.findAll('div'):                                 
            # --------------------------每段評論的標題---------------------------------
                if(len(k.select('h2')) == 1):  
                    _head = k.select('h2')
                    _Stringhead = (str)(_head[0])
                    _Stringhead = _Stringhead.replace('<h2>','')
                    _Stringhead = _Stringhead.replace('</h2>','')
                    #print(_Stringhead) #印出標題  
                    
            # -----------------------------------------------------------------------
            
                s = k.select('img')
                if(len(s)): #判斷是否為空陣列 ==> 裡面為有
                    if(len(s) == 2):#代表該作者有給評分
                        ppp = s[1]['alt'].split("/") 
                        aaaa = ppp[0]  #fsadfsdfsdfasdfsdfasdfsd
                        listscore.append(aaaa)                      
                        #print(aaaa)                       
                    else:     #代表該作者沒有給評分 讓list為null
                        listscore.append("NULL")
                        #print("----------------------------NULL---------------------------------")                        
            #print(listscore) #資料庫~~~~~~~~~~~~~~~~~~~~~~~~~~
            
            
        # ---------------------------------查詢有無下頁(評論)------------------------------------------
            for k in j.findAll('tr'):            
                for q in k.findAll('td' , nowrap = '1'):                    
                    isNext = k.select('img')
                    if(len(isNext) == 2):  #中間頁數判斷
                        if(isNext[1]['alt'] == '[Next]'): #有下一頁就做下一頁 
                            Link_To_Comments = requests.get('http://www.imdb.com'+URL_Link_To_Comments+'/reviews?start='+(str)(page*10))                            
                            #print("OKOK")
                            #print(page)
                            break
                            #http://www.imdb.com/title/tt2119532/reviews?start=10 第2頁                            
                        else:
                            #print("Wrong1")
                            status = False                            
                    elif(len(isNext) == 1):  #判斷最後一頁跟第一頁
                        if(isNext[0]['alt'] == '[Prev]'):
                            #print("Wrong2")
                            status = False                            
                        elif(isNext[0]['alt'] == '[Next]'): #第一頁 有下一頁
                            #print("OKOK")
                            #print(page)
                            Link_To_Comments = requests.get('http://www.imdb.com'+URL_Link_To_Comments+'/reviews?start='+(str)(page*10))                            
                            break
            page += 1
                        
        #---------------------------------------------------------------------------------------                        
    
    # 換電影評論
    cnt+=1    
    if(cnt <= 100):
        print(comment_author_list) 
        print(listscore)
        ggg = headtitle.pop(0)        
        print(ggg)
        print("=================================")
        for length in range(0,len(comment_author_list)):                
            ccc = comment_author_list.pop(0)
            ddd = listscore.pop(0)
            #print(ccc)
            #print(ddd)
            ret = [ggg,ccc,ddd]
            cur.execute(sql,ret)       
    else:
        break
con.commit()
con.close()    