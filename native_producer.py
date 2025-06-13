import time
import json
import random
import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse


count_main = 0
link_list = []
link_dict = dict()

try:
    os.remove('./input/native_urls.txt')
except OSError:
    pass
f = open('./input/native_urls.txt', 'w')


def deep_crawl(crawler_session, url, scheme, netloc):
    global count_main, link_list, link_dict
    count_main += 1
    time.sleep(2)
    print(count_main, " - ", url)
    base_url = scheme + "://" + netloc
    head = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}
    try:
        response = crawler_session.get(url.strip(), headers=head, timeout=10)

        mime_type = response.headers
        if "html" in mime_type['Content-Type']:
            soup = BeautifulSoup(response.content, 'lxml')
            a_tags = soup.body.find_all("a")
            count = 0
            for link in a_tags:
                count += 1
                attr_name = 'src' if link.get('src') else 'href' if link.get('href') else None
                fullurl = link.get(attr_name)
                fullurl_copy = fullurl
                if not fullurl or fullurl[0] == "#":
                    continue
                if 'http' not in fullurl and 'https' not in fullurl:  # filter urls with "#"
                    fullurl = base_url + fullurl
                if fullurl_copy[:2] == "//":
                    fullurl = scheme + ":" + fullurl_copy
                a_parse = urlparse(fullurl)
                if netloc == a_parse.netloc:
                    if fullurl not in link_dict:
                        link_dict[fullurl] = ""
                        link_list.append(fullurl)
                        print(fullurl)
                        send_dict = dict()
                        send_dict[fullurl] = ""
                        f.write(fullurl+ '\n')
    except Exception as e:
        print("Parsing error", e)
    print("Length =", len(link_dict))

    if len(link_dict) >= 50:
        raise Exception("Crawling done for", netloc)
    deep_crawl(crawler_session, link_list[count_main], scheme, netloc)


def main():
    global count_main, link_list, link_dict
    crawler_session = requests.Session()
    p = 'http://lum-customer-empmonitor-zone-poweradspy_all_team_dc_ip:rp5ldwq1inn4@zproxy.lum-superproxy.io:22225'
    proxies = {'https': p, }
    crawler_session.proxies = proxies
    head = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}

    with open('./input/native_domains.txt') as f:
        lines = list(f)
    random.shuffle(lines)
    for url1 in lines:
        print(url1)
        count_main = len(link_list) - 1
        link_dict = dict()
        try:
            url_break_res = crawler_session.get(url1, headers=head, timeout=10)
            url_break = urlparse(url_break_res.url)
            deep_crawl(crawler_session, url1, url_break.scheme, url_break.netloc)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
    f.close()