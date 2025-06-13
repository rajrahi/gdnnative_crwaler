import os
import re
import time
import json
import base64
import logging
import hashlib
import shutil
import requests
import tempfile
import random
import threading
import imagehash
import tldextract
import unicodedata
import pytesseract
import accept
from PIL import Image
from io import BytesIO
from helpers import init, redirect
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool
from urllib.parse import urlparse
from selenium.common import exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from logging.handlers import RotatingFileHandler
from adblockparser import AdblockRules
from multiprocessing_logging import install_mp_handler
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from prometheus_client import start_http_server, multiprocess, REGISTRY, generate_latest, Counter

# global variable
list_of_json = []
net = ""
recursive_count = 0
destn_url = []
ad_text = ""

_tempdir = tempfile.mkdtemp()
os.environ['prometheus_multiproc_dir'] = _tempdir

class recursiveException(Exception):
    def __init__(self, value):
        self.val = value

def setup_logger(name, filename):
    logger = logging.getLogger(name)
    handler = RotatingFileHandler(filename, encoding="utf-8", maxBytes=104960000, backupCount=50)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class Metrics(object):
    def __init__(self, idx):
        self._tempdir = _tempdir
        self._registry = REGISTRY
        self._multiprocess_registry = multiprocess.MultiProcessCollector(self._registry, self._tempdir)
        
        self.processed_gdn_url = Counter('processed_gdn_url_count', 'Number of urls processed', ['country', 'status', 'source'],
                                         registry=self._registry)
        self.processed_gdn_ad = Counter('processed_gdn_ad_count', 'Number of ads based on API response',
                                        ['country', 'message', 'source'], registry=self._registry)
        self.processed_gdn_api_hit = Counter('processed_gdn_api_hit_count', 'Number of geo api hits',
                                             ['country', 'status'], registry=self._registry)
  
        self.processed_native_url = Counter('processed_native_url_count', 'Number of urls processed',
                                            ['country', 'status', 'source'],
                                            registry=None)
        self.processed_native_ad = Counter('processed_native_ad_count', 'Number of ads based on API response',
                                           ['country', 'message', 'source'],
                                           registry=None)
        self.processed_native_api_hit = Counter('processed_native_api_hit_count', 'Number of geo api hits',
                                   ['country', 'status'],
                                   registry=None)
        self.processed_native_adnetowrk = Counter('processed_native_ad_network_status', 'Adnetwork status',
                                           ['adnetwork', 'message', 'source'],
                                           registry=None) 
                                   
    def shutdown(self):
        # remove the temporary directorys
        shutil.rmtree(self._tempdir)

    def collect(self):
        return generate_latest(self._registry).decode()


# Send json to API using POST:
def send_json(payload, gdn_logger, native_logger, proxy, metric):
    if payload["network"] != "gdn":
        payload["ad_title"] = unicodedata.normalize("NFKD", payload["ad_title"])
        payload["newsfeed_description"] = unicodedata.normalize("NFKD", payload["newsfeed_description"])
        payload["ad_text"] = unicodedata.normalize("NFKD", payload["ad_text"])
        print_payload = dict()
        # logger.warning(payload)
        for key, value in payload.items():
            if key != "ad_image":   # so that the log file doesnt get overcrowded, as its value is huge.
                if key == "redirect_url":
                    print_payload[key] = [x.encode("utf-8") for x in value]
                else:
                    print_payload[key] = value.encode("utf-8")
        #logger.warning(print_payload)
        native_logger.error(print_payload)
        try:
            # DEV: https://native-dev.poweradspy.com/api/insert-ads
            # MAIN: https://nativeapi.poweradspy.com/api/insert-ads
            headers = {'Content-Type': 'application/json'}
            post_json = requests.request("POST", "https://nativeapi.poweradspy.com/api/insert-ads", headers=headers,
                                         data=json.dumps(payload))
            # post_json = requests.request("POST", "https://native-dev.poweradspy.com/api/insert-ads", headers=headers,
            #                              data=json.dumps(payload))
            print("[Native API MESSAGE] ", post_json.text, "Time:", datetime.utcnow())
            native_logger.error(post_json.text)
            log_print = json.loads(post_json.text)
            message = log_print["message"]
            metric.processed_native_ad.labels(country=proxy, message=message, source=payload['source']).inc()
            metric.processed_native_adnetowrk.labels(adnetwork=net, message=message, source=payload['source']).inc()
        except Exception as e:
            metric.processed_native_ad.labels(country=proxy, message="other errors", source=payload['source']).inc()
            metric.processed_native_adnetowrk.labels(adnetwork=net, message="API errors", source=payload['source']).inc()
            print("Native API error : ", e)
            return 0
        return 1
    else:
        gdn_logger.warning(payload)
        try:
            # DEV: https://gdn-dev.poweradspy.com/api/insert-ads
            # MAIN: https://gdnapi.poweradspy.com/api/insert-ads
            if len(payload['post_owner'].strip()) > 0:
                post_json = requests.request("POST", "https://gdnapi.poweradspy.com/api/insert-ads", data=payload)
                print("[GDN API RESP.] ", post_json.text, "Post_owner:", payload['post_owner'], "ad_id:", payload['ad_id'])
                gdn_logger.error(post_json.text)
                log_print = json.loads(post_json.text)
                message = log_print["message"]
                metric.processed_gdn_ad.labels(country=proxy, message=message, source=payload['source']).inc()
            else:
                print("[GDN SEND JSON ERROR] Post owner null. Destination_url: ", payload['destination_url'])
        except Exception as e:
            metric.processed_gdn_ad.labels(country=proxy, message="other errors", source=payload['source']).inc()
            print("GDN API POST error(other errors): ", e)
            return 0
        return 1

# Fetching image hash, destination URL and post_owner:
def update_json(driver, r_session, payload, gdn_logger, native_logger, proxy, metric, uniq_ad_dict,  base_url, input_url):

    stop_postowner = ["rev-stripe", "ad-score", "adskeeper", "jubnaadserve", "mgid", "outbrain", "revcontent",
                      "system-loading", "plexop", "adnow", "content-ad", "taboola", "strossle", "popin",
                      "yengo", "logly", "jubna", "mfadsrvr","newsmaxwidget","speakol"]
                      
    if payload['network'] == 'gdn':
        payload['target_site'] = base_url
        payload['placement_url'] = input_url
        try:
            if payload['ad_id'] not in uniq_ad_dict:
                uniq_ad_dict[payload['ad_id']] = ""
                r1 = r_session.get(payload['destination_url'], timeout=80)

                if payload['destination_url'] and "doubleclick" not in urlparse(
                        r1.url).netloc and "googleadservices" not in urlparse(r1.url).netloc:
                    payload['destination_url'] = r1.url
                    x = tldextract.extract(payload['destination_url'])
                    payload['post_owner'] = x.domain
                else:
                    raise Exception("Destination url null")

                send_json(payload, gdn_logger, native_logger, proxy, metric)

        except Exception as e:
            print("Destination URL error!", e)
            payload['post_owner'] = ""
            return 0
    else:
        # Image hash done using dhash" & text hash done using md5
        try:
            if payload['image_url_original'] != "":
                r_img = r_session.get(payload['image_url_original'], timeout=30)
                payload['image_url_original'] = r_img.url
                img12 = Image.open(BytesIO(r_img.content)).convert("RGB")
                hash1 = str(imagehash.dhash(img12))
                hash2 = hashlib.md5(payload['ad_title'].encode())
                payload['ad_id'] = str(hash1 + hash2.hexdigest())
                payload['ad_image'] = str(base64.b64encode(r_session.get(r_img.url, timeout=20).content))[2:-1]
            else:
                payload['type'] = "TEXT"
                hash2 = hashlib.md5(payload['ad_title'].encode())
                payload['ad_id'] = str(hash2.hexdigest())
                payload['ad_image'] = ""
        except Exception as e:
            print("hash/base64 error: ", e)
            #raise Exception("hash/base64 error: ", e)

        # destination using requests
        try:
            if payload['ad_id'] not in uniq_ad_dict:
                uniq_ad_dict[payload['ad_id']] = ""
                r1 = r_session.get(payload['destination_url'], timeout=40)
                if payload['destination_url'] and r1.status_code == 200:
                    x = tldextract.extract(r1.url)
                    payload['post_owner'] = x.domain
                    if payload['post_owner'] in stop_postowner:
                        try:
                            driver.set_page_load_timeout(60)
                            driver.get(payload['destination_url'])
                            u = driver.current_url
                            q = tldextract.extract(u).domain
                            payload['post_owner'] = q
                            payload['destination_url'] = u
                        except:
                            print("owner/url error")
                    else:
                        payload['destination_url'] = r1.url
                else:
                    raise Exception("Destination url null OR status code: ", r1.status_code)
                #ret_redirects = get_redirects(payload['redirect_url'], payload['destination_url'], r_session)
                ret_redirects = redirect.get(driver, r_session, payload)
                payload['redirect_url'] = ret_redirects
                if payload['post_owner'] not in stop_postowner:
                    #print(payload)
                    send_json(payload, gdn_logger, native_logger, proxy, metric)
        except Exception as e:
            print("Destination URL error!", e)
            payload['post_owner'] = ""
            return 0


# Storing json template:
def make_json(red_url, d_url, img, title, ad_text, ad_brand, ip_dict, base_url, input_url, metric,img_hash, base, img_size, network="gdn"):
    
    dict_to_json = {
            "redirect_url": "",
            "placement_url": "",
            "image_url_original": "",
            "ad_number_position": "1",
            "post_owner_image": "",
            "post_date": "",
            "post_owner": "",
            "ad_id": "",
            "city": "",
            "network": "",
            "platform": "12",
            "ad_text": "",
            "state": "",
            "version": "1.0.0",
            "type": "IMAGE",
            "ad_position": "SIDE",
            "ad_image": "",
            "destination_url": "",
            "ad_title": "",
            "country": "",
            "newsfeed_description": "",
            "source": "android",
            "ip_address": "",
            "target_site": ""}
    
    dict_to_json.update({"redirect_url": red_url})
    dict_to_json.update({"placement_url": input_url})
    dict_to_json.update({"destination_url": d_url})
    dict_to_json.update({"image_url_original": img})
    dict_to_json.update({"ad_text": ad_text})
    dict_to_json.update({"network": network})
    dict_to_json['target_site'] = base_url
    
    dict_to_json["ip_address"] = ip_dict['ipAddress']
    dict_to_json["country"] = unicodedata.normalize('NFKD', ip_dict['countryName']).encode('ascii', 'ignore').decode("utf-8")
    dict_to_json['state'] = unicodedata.normalize('NFKD', ip_dict['stateProv']).encode('ascii', 'ignore').decode("utf-8")
    dict_to_json['city'] = unicodedata.normalize('NFKD', ip_dict['city']).encode('ascii', 'ignore').decode("utf-8")
    
    
    if network == "gdn":
        dict_to_json.update({'ad_id': img_hash})
        dict_to_json.update({'ad_image': base})
        dict_to_json["ad_image_size"] = img_size
        dict_to_json["ad_sub_position"] = "TOP"
        dict_to_json['post_date'] = str(time.time())
        
        list_of_json_gdn.append(dict_to_json)
    else:
        dict_to_json.update({"ad_title": title})
        
        ad_brand_list = ad_brand.split("|")
        if len(ad_brand_list) > 1:
            brand = ad_brand_list[0].strip() if "Sponsored" not in ad_brand_list[0] else ad_brand_list[1].strip()
        else:
            if "Sponsored by" in ad_brand_list[0]:
                brand = ad_brand_list[0][13:].strip()
            else:
                ad_brand_list = ad_brand.split("-")
                brand = ad_brand_list[1].strip() if len(ad_brand_list) > 1 else ad_brand_list[0].strip()

        dict_to_json.update({"newsfeed_description": brand})

        dict_to_json['post_date'] = str(datetime.utcnow().timestamp()).split('.', 1)[0]
        dict_to_json['first_seen'] = str(datetime.utcnow().timestamp()).split('.', 1)[0]
        dict_to_json['last_seen'] = str(datetime.utcnow().timestamp()).split('.', 1)[0]
     
        list_of_json_native.append(dict_to_json)

def element_screenshot(driver, element):
        window_height = driver.get_window_size()['height']
        element_height = element.size['height']
        y_gap = (window_height - element_height) / 2
        y_scroll = element.location['y'] - y_gap
        if y_scroll <= 0:
            y_scroll = 0
        x_scroll = element.location['x']
        driver.execute_script("window.scrollTo(arguments[0], arguments[1]);", x_scroll, y_scroll)
        time.sleep(0.5)
        try:
            image = element.screenshot_as_png
        except:
            return "", "", ""
        pil_image = Image.open(BytesIO(image))
        hash1 = imagehash.dhash(pil_image)
        # pil_image.save(dest_img+ str(hash1) + ".png")
        width, height = pil_image.size
        img_size = str(width) + "*" + str(height)
        buffered = BytesIO()
        pil_image.save(buffered, format="png")
        encoded = base64.b64encode(buffered.getvalue())
        base = str(encoded)[2:-1]
        return str(hash1), str(base), img_size


def iframe_recursive(driver, r_session, x, ip_dict, base_url, input_url, metric):
    global recursive_count, destn_url
    recursive_count += 1
    if recursive_count >= 50:
        raise recursiveException(-1)
    
    raw_rules = open('easylist.txt', 'r')  # Adblocker filters
    rules = AdblockRules(raw_rules)
    stop_size = ["200200", "240400", "250250", "250360", "300250", "336280", "580400", "120600", "160600", "300600",
                 "3001050", "46860", "72890", "930180", "97090", "970250", "980120", "30050", "32050", "320100",
                 "750100", "750200", "750300", "32050", "320100"]
    stop_logo = 'https://tpc.googlesyndication.com/pagead/images/abg/en.png'
    stop_icon = "https://tpc.googlesyndication.com/pagead/images/abg/icon.png"
    stop_gif = "https://s0.2mdn.net/dot.gif"
    stop_text = ["Ad closed by", "Report this ad", "Why this ad?", "Why this ad?\\xa0",
                 "Seen this ad multiple times",
                 "Not interested in this ad", "Ad covered content", "Ad was inappropriate", "Stop seeing this ad",
                 "We'll try not to show that ad again",
                 "Thanks. Feedback improves Google ad", "Thanks. Feedback improves Google ads",
                 "Ad was too personal"]
    try:
        frames = driver.find_elements(by='tag name', value='iframe')
    except:
        frames = []

    if len(frames) >= 10:
        driver.switch_to.default_content()
        raise recursiveException(-1)

    if len(frames) == 0:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        text = str(soup)
        driver.switch_to.parent_frame()
        z = [x for x in soup.find_all("a") if len(x.find_all("img")) != 0]
        for y in z[:1]:
            img = y.find_all('img')
            if y.has_attr('href') and rules.should_block(y['href']) and init.url_checker(img[0]['src']) and img[0][
                'src'] != stop_icon and img[0]['src'] != stop_logo and img[0]['src'] != stop_gif:
                try:
                    r_img = requests.get(img[0]['src'], timeout=30)
                    content_type = r_img.headers['content-type']
                    if int(r_img.headers['content-length']) > 5120000 or content_type.split("/")[0] != 'image':
                        raise Exception('size or type error!')
                    img12 = Image.open(BytesIO(r_img.content))
                except Exception as e:
                    print("Image load error:", e)
                    raise recursiveException(-1)

                width, height = img12.size
                if str(width) + str(height) not in stop_size:
                    try:
                        a_tags = soup.body.find_all(
                            "a")
                    except:
                        a_tags = []
                    links = []
                    for l in a_tags:
                        a = 'src' if l.get('src') else 'href' if l.get('href') else None
                        links.append(l.get(a))
                    destn_url = links
                    raise recursiveException(1)
                hash1 = imagehash.dhash(img12)
                base = str(base64.b64encode(requests.get(img[0]['src'], timeout=30).content))[2:-1]
                make_json(y['href'], y['href'], img[0]['src'], '', '', '', ip_dict, base_url, input_url, metric, str(hash1), base, str(width) + "*" + str(height))
                 
                raise recursiveException(0)
        try:
            a_tags = soup.body.find_all("a")
        except:
            a_tags = []
        links = []
        k1 = 0
        for l in a_tags:
            a = 'src' if l.get('src') else 'href' if l.get('href') else None
            links.append(l.get(a))
            if l.get(a) is not None:
                net = urlparse(l.get(a))
                if "google" in str(net.netloc).lower():
                    k1 = 1

        for url in links:
            if rules.should_block(str(url)) and 'google' in text.lower() and k1 == 1:
                text1 = soup.body.find_all("span")
                text2 = soup.body.find_all("a")
                outp = set()
                destn_url = links
                for t in text2 + text1:
                    if t.get_text().strip() not in stop_text:
                        outp.add(t.get_text())
                if len(outp) < 100:
                    global ad_text
                    ad_text = " ".join(outp)
                    raise recursiveException(1)


    else:
        for num in range(len(frames)):
            try:
                wait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(frames[num]))
            except exceptions.TimeoutException:
                continue
            iframe_recursive(driver, r_session, x, ip_dict, base_url, input_url, metric)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        text = str(soup)
        driver.switch_to.parent_frame()
        z = [x for x in soup.find_all("a") if len(x.find_all("img")) != 0]
        for y in z[:1]:
            img = y.find_all('img')
            if y.has_attr('href') and rules.should_block(y['href']) and init.url_checker(img[0]['src']) and img[0][
                'src'] != stop_icon and img[0]['src'] != stop_logo and img[0]['src'] != stop_gif:
                try:
                    r_img = requests.get(img[0]['src'], timeout=30)
                    content_type = r_img.headers['content-type']
                    if int(r_img.headers['content-length']) > 5120000 or content_type.split("/")[0] != 'image':
                        raise Exception('size or type error!')
                    img12 = Image.open(BytesIO(r_img.content))
                except Exception as e:
                    print("Image load error:", e)
                    raise recursiveException(-1)
                width, height = img12.size
                if str(width) + str(height) not in stop_size:
                    try:
                        a_tags = soup.body.find_all("a")
                    except:
                        a_tags = []
                    links = []
                    for l in a_tags:
                        a = 'src' if l.get('src') else 'href' if l.get('href') else None
                        links.append(l.get(a))
                    destn_url = links
                    raise recursiveException(1)
                hash1 = imagehash.dhash(img12)
                base = str(base64.b64encode(requests.get(img[0]['src'], timeout=30).content))[2:-1]
                make_json(y['href'], y['href'], img[0]['src'], '', '', '', ip_dict, base_url, input_url, metric, str(hash1), base, str(width) + "*" + str(height))
                
                raise recursiveException(0)

        a_tags = soup.body.find_all("a")
        links = []
        k2 = 0
        for l in a_tags:
            a = 'src' if l.get('src') else 'href' if l.get('href') else None
            links.append(l.get(a))
            if l.get(a) is not None:
                net = urlparse(l.get(a))
                if "google" in str(net.netloc).lower():
                    k2 = 1

        for url in links:
            if rules.should_block(str(url)) and 'google' in text.lower() and k2 == 1:
                text1 = soup.body.find_all("span")
                text2 = soup.body.find_all("a")
                outp = set()
                destn_url = links
                for t in text2 + text1:
                    if t.get_text().strip() not in stop_text:
                        outp.add(t.get_text())
                if len(outp) < 100:
                    ad_text = " ".join(outp)
                    raise recursiveException(1)


def main(driver, r_session, input_url, base_url, ip_dict, gdn_logger, native_logger, offset, proxy, metric, uniq_ad_dict):

    global list_of_json_gdn, list_of_json_native, net, ad_text, recursive_count
    start_time = time.time()
    list_of_json_gdn = []
    list_of_json_native = []
    recursive_count = 0
    ad_text = ""
    
    # Hitting the input URL.
    try:
        driver.get(input_url.strip())
    except Exception as e:
        raise Exception("Url loading error:", e, "\n", input_url.strip())
    
    try:
    # Try clicking the element with either aria-label 'Aceptar' or 'Consent'

        driver.find_element(By.XPATH, "//button[@aria-label='Aceptar' or @aria-label='Consent' or @aria-label='Accept All' or @aria-label='Accept' or @aria-label='Alle Akzeptieren']").click()
        time.sleep(2)
    except:
        pass

        
    #Uptdate message in log file
    log_var = "Offset:" + str(offset) + " " + input_url.strip()
    gdn_logger.warning(log_var)
    native_logger.warning(log_var)
    
    # Screen Scrolling
    try:
        height = driver.execute_script("return document.body.scrollHeight")
        x = 0
        while x < height and x <= 10000:
            height = driver.execute_script("return document.body.scrollHeight")
            x += 400
            driver.execute_script("window.scrollTo(0, arguments[0]);", x)
            time.sleep(0.2)
        driver.execute_script("window.scrollTo(0,0);")
    except:
        pass
        
    # Accepting Cookies permission
    try:
        accept.accept_cookie(driver, base_url)
    except:
        pass
    
    # creating a soup object
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    
    
    #.........................................................................GDN................................................................
    
    try:
        #Getting all displayed iframes
        body_tag = driver.find_element(by='tag name', value='body')
        iframe = body_tag.find_elements(by='tag name', value='iframe')
        displayed_frames = []
        for x in range(len(iframe)):
            try:
                if iframe[x].is_displayed():
                    displayed_frames.append(x)
            except:
                continue
        
        for x in displayed_frames:
            global destn_url
            destn_url.clear()
            ad_text = ""
            try:
                wait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(x))
            except exceptions.TimeoutException:
                continue
            try:
                #Iframe recursion started
                iframe_recursive(driver, r_session, x, ip_dict, base_url, input_url, metric)
            except recursiveException as flag:
                driver.switch_to.default_content()
                
                #If ads is not an image
                if int(flag.val) == 1:
                    try:
                        for link in destn_url:
                            if link == "#":
                                continue
                            if link is not None:
                                resp = r_session.get(link)
                                destination = resp.url
                                redirect = link
                                destination_netloc = urlparse(destination)
                                if redirect != destination and "google" not in str(
                                        destination_netloc.netloc).strip().lower() and str(
                                    destination_netloc) not in base_url:
                                    
                                    # Taking ScreenShot of the element
                                    img_hash, base, img_size = element_screenshot(driver, iframe[x])
                                    if str(img_hash).strip() == "0000000000000000" or str(img_hash) == "":
                                        raise Exception("Screenshot is a blank img")
                                    img_width, img_height = map(lambda itr: int(itr), img_size.split("*"))
                                    if (img_height < 65 and img_width < 65) or (img_width > 1281 or img_height > 1201):
                                        raise Exception("Screenshot size rejected", "width", img_width, "height",
                                                        img_height)
                                    make_json(redirect, destination, '', '', '', '', ip_dict, base_url, input_url, metric, img_hash, base, img_size)
                                    
                                    break
                    except Exception as e:
                        print("Screenshot error: ", e)

            driver.switch_to.default_content()
            
    except:
       pass
        
        
     
    #.........................................................................NATIVE................................................................

    # OUTBRAIN NETWORK
    tb1 = [x for x in soup.find_all("a", {"href".lower(): re.compile("(outbrain)")})]
    tb2 = [x for x in soup.find_all("a", {"onmousedown".lower(): re.compile("(outbrain)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                href = y['href']
                z = urlparse(href)
                netloc = z.netloc
                if netloc != "www.outbrain.com":
                    i1 = y.find('span',
                                attrs={'class': re.compile("(ob-rec-image-container)")})  # edited for both img and video
                    if i1:
                        i = i1.img['src'] if i1.img else i1.video.source['src']
                    else:
                        i = ""
                    #             i1 = y.find('span', attrs={'class': 'ob-unit ob-rec-image-container'})
                    #             i = i1.img['src'] if i1 else ""
                    if y.span.img:
                        try:
                            t = y.img['title']
                        except:
                            t = y.img['alt']
                    elif y.span:
                        t = y.find('span', {'class': 'ob-unit ob-rec-text'})['title']  # added aatr
                    else:
                        t = y.text.strip()
                        t = " ".join(t.split())
                    brand_ele = y.find(class_='ob-rec-source')
                    brand_ele1 = y.find(class_='ob-unit ob-rec-source')
                    ad_brand = brand_ele.text if brand_ele else brand_ele1.text if brand_ele1 else ""
                    net = "outbrain"
                    text = ""
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                    
            except Exception as e:
                print("outbrain network error", e)
                continue

    # TABOOLA NETWORK
    tb = [x for x in soup.find_all("div", {"observeid".lower(): re.compile("(tbl-observe)")})]
    if tb:
        for y1 in tb:
            try:
                #         tb1 = [x1 for x1 in y.findAll('a')]
                #         for y1 in tb1:
                href = y1.a['href']
                z = urlparse(href)
                netloc = z.netloc
                if netloc != "popup.taboola.com":
                    img = re.findall(r'(https?://images\S+)', str(y1))
                    if img:
                        i = img[0].replace("\"", " ").replace("\'", " ").split(" ")[0]
                    else:
                        i = ""
                    t = y1.a['title'].strip() if y1.a.has_attr('title') else y1.a.text.strip()  # added a
                    brand_ele = y1.find(class_='branding')

                    brand_ele1 = y1.find(class_='branding-inner')
                    ad_brand = brand_ele.text if brand_ele else brand_ele1.text if brand_ele1 else ""

                    net = "taboola"
                    text = ""
                    if t:  # added
                        make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                        
            except Exception as e:
                print("Taboola block error %s in input url %s", e, input_url)
                continue

    # STROSSLE
    tb1 = [x for x in soup.find_all("div", {"class".lower(): re.compile("(col-xs-12)")})]
    tb2 = [x for x in soup.find_all("div", {"class".lower(): ("spklw-post")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                href = y.a['href']
                z = urlparse(href).netloc
                if z == "strossle.it":
                    driver.get(href)
                    soup_str = BeautifulSoup(driver.page_source, 'html.parser')
                    link_tag = soup_str.find('a', attrs={'data-position': '1'})
                    href = link_tag['href']
                if y.img:
                    i = y.img['src']
                else:
                    img = y.find('div', attrs={'class'.lower(): re.compile("(spklw-post-image)")})
                    img1 = re.findall(r'(//\S+)', str(img))
                    i = img1[0].replace("\"", " ").replace("\'", " ").replace("amp;", "").split(" ")[0]
                    i = 'https:' + i
                if i == "https://assets.strossle.com/images/p.png":
                    i = ""
                #             title = y.find('div', attrs={'class': re.compile("post-title")})
                #             t = title.text.strip()
                title1 = y.find('div', attrs={'class': re.compile("post-title")})
                title2 = y.find('div', attrs={'class': re.compile("_title")})
                t = title1.text.strip() if title1 else title2.text.strip()
                ad_brand = ""
                net = "strossle"
                text = ""
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                # print("Strossle block error %s in input url %s", e, input_url)
                continue

    # ZERGNET
    tb = [x for x in soup.find_all('div', {'class': 'zergentity'})]
    if tb:
        for y in tb:
            try:
                if y.a.has_attr('href'):
                    i = y.img['src']
                    t = y.text.strip() 
                    ad_brand= ""
                    if y.span:
                        t = " ".join(y.text.strip().split()).strip(y.span.text)
                        ad_brand= y.span.text
                    net = "Zergnet"
                    text = ""
                    if "io" in y.a['href']:
                        href = y.a['href']
                    else:
                        driver.get(y.a['href'])
                        soup_zer = BeautifulSoup(driver.page_source, 'html.parser')
                        link_tag = soup_zer.find('div', attrs={'class': 'item first'})
                        href = link_tag.a['href']
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Zergnet block error %s in input url %s", e, input_url)
                continue

    # Adblade Ads
    tb1 = [x for x in soup.find_all("div", {"class": 'ad'})]
    tb2 = [x for x in soup.find_all("table", {"class": 'ad'})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                if y.a.has_attr('href'):
                    href = y.a['href']
                    img = re.findall(r'(https?://static-cdn.adblade\S+)', str(y))
                    if img:
                        i = y.img['src']
                        title_ele = y.find('div', {'class': 'text'})  
                        #                     title_ele1 = title_ele.find('a')
                        title_ele2 = y.find('a', {'class': 'description'})
                        t = title_ele.a.text if title_ele else title_ele2.text if title_ele2 else y.text
                        brand_ele = y.find('a', {'class': 'displayname'})
                        ad_brand = brand_ele.text if brand_ele else ""
                        net = "adblade"
                        text = ""
                        make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                # print("adblade block1 error %s in input url %s", e, input_url )
                continue

    # PLISTA
    tb1 = [x for x in soup.find_all("a", attrs={"href".lower(): re.compile("(plista.com/re)")})]
    tb2 = [x for x in soup.find_all("a", attrs={"class".lower(): re.compile("(plista_widget)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                if y.has_attr('href'):
                    img = re.findall(r'(https?://media.plista\S+)', str(y))
                    if img:
                        i = img[0].replace("\"", " ").replace("\'", " ").split(" ")[0]
                    else:
                        i = ""
                    if y.span.img:
                        t = y.span.img['title'].split('&')[0]
                    else:
                        t = " ".join(y.text.strip().split())
                    net = "plista"
                    text_div = y.find("span", {"class": "itemText"})
                    ad_brand = ""
                    if text_div:
                        text = text_div.text.strip()
                    else:
                        text = ""
                    if y['href'] != 'https://www.plista.com/de':
                        make_json(y['href'], y['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Plista block error %s in input url %s", e, input_url)
                continue

    # Yahoo Gemini for (msn.com)
    tb = [x for x in soup.find_all("li", {"class": re.compile("(gemini-item)")})]
    if tb:
        for y in tb:
            try:
                hrefe = y.find('a', {'href'.lower(): re.compile('(gemini.yahoo)')})
                if hrefe:
                    href = hrefe['href']
                    i = y.img['src']
                    if i:
                        t = y.find('a', {'class': re.compile('(LineClamp)')}).text.strip()
                        net = "Yahoo Gemini"
                        textele = y.find('p', {'class': re.compile('(LineClamp)')})
                        text = textele.text.strip() if textele else ""
                        ad_brand = hrefe.text if True else ""
                        make_json(y['href'], y['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Yahoo block1 error %s in input url %s", e, input_url)
                continue

    # Yahoo Gemini for yahoo.com/news
    tb = [x for x in
          soup.find_all("div", {"class": re.compile("(gemini-ad)")})]
    if tb:
        for y in tb:
            try:
                link_gemini = y.find('a', attrs={'href'.lower(): re.compile("(gemini)")})
                if link_gemini:
                    img = re.findall(r'(https?://s.yimg.com\S+)', str(y))
                    i = img[0].replace(")", " ").replace("\"", " ").split(" ")[0]
                    t = y.h3.text
                    net = "Yahoo Gemini"
                    text = y.p.text if y.p else ""
                    ad_brand_e = y.find('a', attrs={'class'.lower(): re.compile("(Mstart)")})
                    ad_brand = ad_brand_e.text if ad_brand_e else ""
                    if i != "https://s.yimg.com/g/images/spaceball.gif":
                        make_json(link_gemini['href'], link_gemini['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Yahoo block2 error %s in input url %s", e, input_url)
                continue

    tb = [x for x in soup.find_all("div", {"class": 'Cf Ov(h) Pos(r) Py(14px) Mt(-3px)'})]
    if tb:
        for y in tb:
            try:
                link_gemini = y.find('a', attrs={'href'.lower(): re.compile("(gemini)")})
                if link_gemini:
                    i = link_gemini.img['src']
                    if i:
                        t = y.h3.text
                        text = y.p.text if y.p.text else ""
                        ad_brand = ""
                        net = "Yahoo Gemini"
                        if i != "https://s.yimg.com/g/images/spaceball.gif":
                            make_json(link_gemini['href'], link_gemini['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Yahoo block3 error %s in input url %s", e, input_url)
                continue

    # ENGAGEYA NETWORK
    tb1 = [x for x in soup.find_all("a", {"href".lower(): re.compile("(engageya)")})]
    tb2 = [x for x in soup.find_all("a", {"onmousedown".lower(): re.compile("(engageya)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                img = y.img['src']
                t = y.text.strip()
                if img and t:
                    href = y['href']
                    if 'http' not in href and 'https' not in href:
                        y['href'] = 'https:' + y['href']
                    i = 'https:' + img
                    net = "engageya"
                    text = ""
                    brand_ele = y.find("span", attrs={'class': re.compile("eng_widget_dn")})
                    ad_brand = brand_ele.text if brand_ele else ""
                    make_json(y['href'], y['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Engageya block error %s in input url %s", e, input_url)
                continue



    # Twiago Network
    tb = [x.parent for x in soup.find_all('div', {"class".lower(): re.compile("twiago--image")})]  # edited here
    if tb:
        for div in tb:
            try:
                href = div.a['href']
                if href:
                    i = div.img['src']
                    t = div.find('a', attrs={"class".lower(): re.compile("twiago--title")}).text
                    # t = div.a.text.strip()
                    # text = div.p.text.strip()
                    text = div.find('a', attrs={"class".lower(): re.compile("twiago--text")}).text
                    net = 'twiago'
                    ad_brand = ''
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("twiago block1 error %s in input url %s", e, input_url)
                continue

    # Midas
    tb1 = [x for x in soup.find_all("ul", {"id".lower(): re.compile("(midas)")})]
    tb2 = [x for x in soup.find_all("ul", {"class".lower(): re.compile("(midas)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for ul in tb:
            for li in ul.find_all('li'):
                try:
                    href = li.a['href']
                    i = li.img['src']
                    t = li.text.strip()
                    text = ""
                    net = 'midas'
                    ad_brand = ""
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("Midas block error %s in input url %s", e, input_url)
                    continue

    # PubExchange Network
    tb1 = [x for x in soup.find_all("li", {"class".lower(): re.compile("(pe-article)")})]
    tb2 = [x for x in soup.find_all("div", {"class".lower(): re.compile("(p-article)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                href = y.a['href']
                i = y.img['src']
                title = y.find('a', attrs={'class': 'pe-headline'})
                if title:
                    t = title.text.strip()
                else:
                    t = " ".join(y.text.strip().split())
                net = "pubexchange"
                text = ""
                ad_brand = ""
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("PubExchange block error %s in input url %s", e, input_url)
                continue

    # PowerInbox Network
    tb = [x for x in soup.find_all("a", {"href".lower(): re.compile("(stripe.rs)")})]
    if tb:
        for y in tb:
            try:  # added try except
                if y.has_attr('href') and 'branding' not in y['href']:  # edited
                    i = y.img['src']
                    r_img = requests.get(i, timeout=30)
                    img12 = Image.open(BytesIO(r_img.content))
                    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract'
                    try:
                        t = pytesseract.image_to_string(img12).strip()
                    except:
                        t = ""
                    net = "PowerInbox"
                    text = ""
                    ad_brand = ""
                make_json(y['href'], y['href'], r_img.url, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Powerindex error %s in input url %s", e, input_url)
                continue



    # MGID NETWORK
    img = driver.find_elements(By.XPATH, "//img[contains(@class,'mcimg')]")
    for ele in img:
        try:
            actionChains = ActionChains(driver)
            actionChains.context_click(ele).perform()
        except:
            continue
    soup_mgid = BeautifulSoup(driver.page_source, 'html.parser')

    tb = [x for x in soup_mgid.find_all('div', attrs={'class': 'image-with-text'})]
    if tb:
        for y in tb:
            try:
                if y.a.has_attr('href'):
                    i = y.img['src'].replace('*', "")
                    if i:
                        title = y.find('div', attrs={'class': 'mctitle'})
                        t = title.text.strip()
                        net = "mgid"
                        text = ""
                        brand_ele = y.find('div', attrs={'class': 'mcdomain'})
                        ad_brand = brand_ele.text if brand_ele else ""
                        if 'http' not in y.a['href'] and 'https' not in y.a['href']:
                            y.a['href'] = 'https:' + y.a['href']
                            driver.get(y.a['href'])
                            mgid_base = 'https://www.mgid.com'
                            soup1 = BeautifulSoup(driver.page_source, 'html.parser')
                            link_tag = soup1.find('a', {'href': re.compile("(ghits)")})
                            mgid_full = mgid_base + link_tag['href']
                            make_json(mgid_full, mgid_full, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                        elif 'ghits' in y.a['href']:  # to check when it doesnot load
                            driver.get(y.a['href'])
                            mgid_base = 'https://www.mgid.com'
                            soup2 = BeautifulSoup(driver.page_source, 'html.parser')
                            link_tag = soup2.find('a', {'href': re.compile("(ghits)")})
                            mgid_full = mgid_base + link_tag['href']
                            make_json(mgid_full, mgid_full, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                        else:
                            make_json(y.a['href'], y.a['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Mgid block error %s in input url %s", e, input_url)
                continue

    # REVECONTENT Ads
    tb = [x for x in soup.find_all("a", {"href".lower(): re.compile("(revcontent)")})]
    if tb:
        for y in tb:
            try:
                href = y['href']
                img_con = y.find('div', attrs={'class': re.compile('rc-photo')})
                img = re.findall(r'(https?://\S+)', str(img_con))
                if img:
                    i = img[0].replace("\"", " ").replace("\'", " ").split(" ")[0]
                    try:
                        i = i.split("&")[0]
                    except:
                        pass
                    title_ele1 = y.find('div', attrs={'class': re.compile('rc-headline')})
                    title_ele2 = y.find('h4', attrs={'class': re.compile('rc-headline')})
                    t = title_ele1.text if title_ele1 else title_ele2.text
                    brand_ele = y.find('div', attrs={'class': re.compile('rc-provider')})
                    ad_brand = brand_ele.text
                    net = "revcontent"
                    text = ""
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Revcontent block error %s in input url %s", e, input_url)
                continue

    # CONTENT-AD NETWORK
    tb = [x for x in soup.find_all("div", {"class": "ac_container"})]
    if tb:
        for y in tb:
            try:
                href = y.a['href']
                i = y.img['src']
                title_ele = y.find("div", {"class": "ac_title"})
                t = title_ele.text if title_ele else y.img['title']
                brand_ele = y.find("div", {"class": "ac_referrer"})
                ad_brand = brand_ele.text if brand_ele else ""
                net = "content-ad"
                text = ""
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Content_ad block error %s in input url %s", e, input_url)
                continue

    # YENGO NETWORK
    tb = [x for x in soup.find_all("a", {"href".lower(): re.compile("(yengo)")})]
    if tb:
        for y in tb:
            try:
                net = "yengo"
                text = ""
                #             ad_brand = ""
                if y.has_attr('href'):
                    i = y.img['src']
                    if i:
                        title = y.find('div', attrs={'class': 'grf-list__title'})
                        brand = y.find('div', attrs={'class': 'grf-list__advertiser'})
                        if title:
                            t = title.text.strip()
                        else:
                            t = y.text.strip()
                        if brand:
                            ad_brand = brand.text.strip()
                        else:
                            ad_brand = ""
                        if t:
                            make_json(y['href'], y['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Yengo block error %s in input url %s", e, input_url)
                continue

    # JUBNA NETWORK
    tb = [x for x in soup.find_all("a", {"class".lower(): re.compile("(jb-anchor)")})]  # added
    if tb:
        for y in tb:
            try:
                #             img = y.img['src']
                img_ele = y.find("img", {"class": re.compile("(jb)")})  # added
                img = img_ele['src']  # added
                if img:
                    href = y['href']
                    if 'http' not in href and 'https' not in href:
                        href = 'https:' + href
                        img = 'https:' + img
                    #                 t = y.img['title']
                    t = img_ele['title']  # edited
                    try:  # added
                        ad_brand = y.find("span", {"class": re.compile("(brnd)")}).text
                    except:
                        ad_brand = ""
                    net = "jubna"
                    text = ""
                    #                 ad_brand = ""
                    make_json(href, href, img, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except:
                # print("Jubna block error %s in input url %s", e, input_url)
                continue

    # PopIn Network
    tb1 = [x for x in soup.find_all("div", {"class".lower(): re.compile("(popIn_idx)")})]
    tb2 = [x for x in soup.find_all("a", {"class".lower(): re.compile("(_popIn_recommend_article)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                href = y.a['href'] if y.a else y['href']
                img_div = y.find('div', attrs={'class': "_popIn_recommend_art_img"})
                img = re.findall(r'(image: url\S+)', str(img_div))
                if img:
                    i = img[0].replace("\"", " ").replace("\'", " ").split(" ")[2]
                else:
                    i = ""
                title = y.find('div', attrs={'class': re.compile("(title)")})
                t = title.text.strip()
                brand_ele = y.find('div', attrs={'class': re.compile("(media)")})
                if brand_ele:
                    ad_brand = brand_ele.text.replace('（', '  ').replace('）', '  ').split('  ')[
                        1] if brand_ele.text else ""
                else:
                    ad_brand = ""
                net = "popin"
                text = ""
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("popin block error %s in input url %s", e, input_url)
                continue

    # Logly
    tb = [x for x in soup.find_all("div", {"class": "logly-lift-ad"})]
    if tb:
        for y in tb:
            try:
                href = y.a['href']
                try:
                    img = y.find('div', attrs={'class': "logly-lift-ad-img-inner"})['data-loglysrc']
                except:
                    img = y.find('div', attrs={'class': "logly-lift-ad-img-inner"})['data-src']  # added
                if img:
                    i = 'https:' + img
                else:
                    i = ""
                title = y.find('div', attrs={'class': 'logly-lift-ad-title'})
                t = title.text.strip()
                text = ""
                #         ad_brand = ""
                brand_ele = y.find('div', attrs={'class': re.compile("(logly-lift-ad-body)")})
                if brand_ele:
                    ad_brand = brand_ele.text.replace('（', ' ').replace('）', ' ').split(' ')[1] if brand_ele.text else ""
                else:
                    ad_brand = ""
                net = "logly"
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Logly block error %s in input url %s", e, input_url)
                continue

    # Postquare Ads
    # tb = [x for x in soup.find_all("a", {"onmousedown".lower(): re.compile("(gecko)")})]
    # if tb:
    #     for y in tb:
    #         try:
    #             img = y.img['src']
    #             if img:
    #                 href = y['href']
    #                 if 'http' not in href and 'https' not in href:
    #                     y['href'] = 'https:' + y['href']
    #                 i = 'https:' + img
    #                 title_ele = y.find("span", {"class": "eng_widget_is"})
    #                 t = title_ele.text if title_ele else y.text.strip()
    #                 net = "postquare"
    #                 brand_ele = y.find("span", {"class": "eng_widget_dn"})
    #                 text = ""
    #                 ad_brand = brand_ele.text if brand_ele else ""
    #                 make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
    #         except Exception as e:
    #             print("postquare block error", e)
    #             continue

    # NewsMax Ads
    tb1 = [x for x in soup.find_all("a", {"href".lower(): re.compile("(newsmaxwidget)")})]
    tb2 = [x for x in soup.find_all("a", {"onmousedown".lower(): re.compile("(newsmaxwidget)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            if y.has_attr('href'):
                try:
                    i = y.img['src']
                    t = y.text.strip()
                    t = " ".join(t.split())
                    net = "NewsMax"
                    z = urlparse(y['href'])
                    netloc = z.netloc
                    text = ""
                    ad_brand = ""
                    if netloc != "www.newsmaxfeednetwork.com":
                        make_json(y['href'], y['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("NewsMax block error %s in input url %s", e, input_url)
                    continue

    # SPEAKOL NETWORK
    tb = [x for x in soup.find_all("div", {"class".lower(): re.compile("(sp-mg-l sp-wi-item)")})]
    if tb:
        for y in tb:
            try:
                href = y.a['href']
                i = y.img['src'] if y.img['src'] else y.img['data-src']
                t = y.img['alt'] if y.img['alt'] else y.p.text.strip()
                brand_ele = y.find(class_='sp-sponsor')
                ad_brand = brand_ele.text.strip() if brand_ele else ""
                net = "speakol"
                text = ""
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Speakol block error %s in input url %s", e, input_url)
                continue

    # AdNow Network
    img = driver.find_elements(By.XPATH,"//img[contains(@id,'SC_TBlock')]")
    for ele in img:
        try:
            actionChains = ActionChains(driver)
            actionChains.context_click(ele).context_click().perform()
            time.sleep(1)
        except:
            continue
    soup_adnow = BeautifulSoup(driver.page_source, 'html.parser')
    tb = [x for x in soup_adnow.find_all("div", {"class": re.compile("(SC_TBlock)")})]
    if tb:
        for y in tb:
            ad = [x1 for x1 in y.find_all('a')]
            for y1 in ad:
                try:
                    img = y1.img['src']
                    if 'http' not in img and 'https' not in img:
                        i = y1.find('img', attrs={'style': re.compile("(url)")})
                        img = re.findall(r'(https?://\S+)', str(i))
                        img = img[0].replace("\"", " ").replace("\'", " ").split(" ")[0]
                    if 'http' not in y1['href'] and 'https' not in y1['href']:
                        y1['href'] = 'https:' + y1['href']
                    if y1.has_attr('title'):
                        t = y1['title']
                    else:
                        t = y1.img['alt']
                    net = 'AdNow'
                    text = ""
                    ad_brand = ""
                    make_json(y1['href'], y1['href'], img, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("Adnow block error %s in input url %s", e, input_url)
                    continue

    # Desipearl Network
    tb = [x for x in soup.find_all("a", {"href".lower(): re.compile("desipearl")})]
    if tb:
        for y in tb:
            try:
                href = y['href']
                i = y.img['src']
                try:
                    t = y.text.strip()
                except:
                    t = ("div", {'class': 'related-content-title'}).text
                text = ""
                ad_brand = ""
                net = "desipearl"
                if t:
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
            except Exception as e:
                print("Desipearl block error %s in input url %s", e, input_url)
                continue

    # Colombia Ads fine without iframe
    # soup_dia = BeautifulSoup(driver.page_source, 'html.parser')
    tb1 = [x for x in soup.find_all("a", {"onclick".lower(): re.compile("(clmbtech)")})]
    tb2 = [x for x in soup.find_all("a", {"href".lower(): re.compile("(clmbtech)")})]
    tb = list(set(tb1 + tb2))
    if tb:
        for y in tb:
            try:
                link = re.findall(r'(https?://ade.clmbtech\S+)', str(y))
                href = link[0].replace("\"", " ").replace("\'", " ").split(" ")[0]
                if y.img:
                    i = y.img['src']
                else:
                    i = ""
                t = y.h3.text if y.h3 else y.h4.text if y.h4 else y.text.strip()
                net = "colombia"
                text = ""
                ad_brand = y.p.text if y.p else ""
                make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                print('ad from colombia')
            except Exception as e:
                print("colombia block error %s in input url %s", e, input_url)
                continue


    # Ads inside iframes
 
    body_tag = driver.find_element(By.TAG_NAME, "body") 
    iframe = body_tag.find_elements(By.TAG_NAME, "iframe") 
    displayed_frames = []
    for x in range(len(iframe)):
        try:
            if iframe[x].is_displayed():
                displayed_frames.append(x)
        except:
            continue
    for x in displayed_frames:
        try:
            wait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(x))
        except exceptions.TimeoutException:
            continue
        soup_dia = BeautifulSoup(driver.page_source, 'html.parser')
        driver.switch_to.parent_frame()


        # Dianomi Ads
        tb1 = [x for x in soup_dia.find_all("a", {"href".lower(): re.compile("(dianomi)")})]
        if tb1:
            for y in tb1:
                try:
                    if y.has_attr('href'):
                        try:
                            i = y.img['src']
                        except:
                            i = ""
                        title = y.find('div', attrs={'class': 'maintext'})
                        t = title.text.strip()
                        net = 'dianomi'
                        text = ""
                        try:
                            brand = y.find('div', attrs={'class': 'dianomi_provider_short'})
                            ad_brand = brand.text.strip()
                        except:
                            ad_brand = ""
                        if t:
                            make_json(y['href'], y['href'], i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("Dianomi block error %s in input url %s", e, input_url)
                    continue

        # ADBLADE Ads
        tb1 = [x for x in soup_dia.find_all("div", {"class": 'ad'})]
        tb2 = [x for x in soup_dia.find_all("table", {"class": 'ad'})]
        tb = list(set(tb1 + tb2))
        if tb:
            for y in tb:
                try:
                    if y.a.has_attr('href'):
                        href = y.a['href']
                        img = re.findall(r'(https?://static-cdn.adblade\S+)', str(y))
                        if img:
                            i = y.img['src']
                            title_ele = y.find('td', {'class': 'text'})  # td from div
                            title_ele1 = title_ele.find('a')
                            title_ele2 = y.find('a', {'class': 'description'})
                            t = title_ele1.text if title_ele1 else title_ele2.text if title_ele2 else y.text
                            brand_ele = y.find('a', {'class': 'displayname'})
                            ad_brand = brand_ele.text if brand_ele else ""
                            net = "adblade"
                            text = ""
                            make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    # print("adblade block2 error %s in input url %s", e, input_url)
                    continue

        # Colombia Ads
        tb1 = [x for x in soup_dia.find_all("a", {"onclick".lower(): re.compile("(clmbtech)")})]
        tb2 = [x for x in soup_dia.find_all("a", {"href".lower(): re.compile("(clmbtech)")})]
        tb = list(set(tb1 + tb2))
        if tb:
            for y in tb:
                try:
                    link = re.findall(r'(https?://ade.clmbtech\S+)', str(y))
                    href = link[0].replace("\"", " ").replace("\'", " ").split(" ")[0]
                    imge = re.findall(r'(background:url\S+)', str(y))
                    if y.img:
                        i = y.img['src']
                    elif imge:  # added new
                        i = imge[0].replace(")", " ").replace("(", " ").replace("\"", " ").split(" ")[1]
                    else:
                        i = ""
                    t = y.h3.text if y.h3 else y.h4.text if y.h4 else y.text.strip()
                    net = "colombia"
                    text = ""
                    ad_brand = y.p.text if y.p else ""
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("colombia block error %s in input url %s", e, input_url)
                    continue

        # SPOUTABLE NETWORK
        tb1 = [x for x in soup_dia.find_all("div", {"data-spout-content": "spout-ad"})]
        tb2 = [x for x in soup_dia.find_all("a", {"data-spout-content": "spout-ad"})]
        tb = list(set(tb1 + tb2))
        if tb:
            for y in tb:
                try:
                    i1 = y.find('div', attrs={'class': 'spout-ad-image'})
                    i2 = y.find('a', attrs={'class': 'spout-ad-image'})
                    if i1:
                        img = re.findall(r'(url\S+)', str(i1))
                    elif i2:
                        img = re.findall(r'(url\S+)', str(i2))
                    i = img[0].replace("(", " ").replace(")", " ").split(" ")[1]
                    if i.startswith('//'):
                        i = 'https:' + i
                    href = 'https:' + y['href']
                    title = y.find('div', attrs={'class': re.compile('spout-ad-copy')})
                    t = title.text.strip()
                    net = 'spoutable'
                    text = ""
                    ad_brand = ""
                    make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("colombia block error %s in input url %s", e, input_url)
                    continue

        # Dable Network
        tb = [x for x in soup_dia.find_all("a", {"href".lower(): re.compile("(dable)")})]
        if tb:
            for y in tb:
                try:
                    href = y['href']
                    if y.img:
                        i = y.img['src']
                        if 'https' not in i:
                            i = y.img['data-org-src']
                    else:
                        i = ""
                    t = y.find("div", {"class": "name"}).text.strip()  # edited
                    text = ""
                    net = "dable"
                    try:
                        ad_brand = y.find("span", {"class": re.compile("sp-mark")}).text.strip()  # edited
                    except:
                        ad_brand = ""
                    if t:
                        make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("Dable block error %s in input url %s", e, input_url)
                    continue

        # Twiago ads
        tb = [x.parent for x in soup_dia.find_all('div', {"class".lower(): re.compile("twiago--image")})]
        if tb:
            for div in tb:
                try:
                    href = div.a['href']
                    if href:
                        i = div.img['src']
                        t = div.find('a', attrs={"class".lower(): re.compile("twiago--title")}).text
                        # t = div.a.text.strip()
                        # text = div.p.text.strip()
                        text = div.find('a', attrs={"class".lower(): re.compile("twiago--text")}).text
                        net = 'twiago'
                        ad_brand = ''
                        make_json(href, href, i, t, text, ad_brand, ip_dict, base_url, input_url, metric, '', '', '', net)
                except Exception as e:
                    print("twiago block1 error %s in input url %s", e, input_url)
                    continue

    for n in list_of_json_gdn + list_of_json_native:
        update_json(driver, r_session, n, gdn_logger, native_logger, proxy, metric, uniq_ad_dict, base_url, input_url)

    print("GDN Ads = ", len(list_of_json_gdn), "Native Ads = ", len(list_of_json_native), " Time_taken = ", round(time.time() - start_time, 2))
    mess = "GDN Ads = " + str(len(list_of_json_gdn)) + " " + "Native Ads = " + str(len(list_of_json_native)) + " Time_taken = " + str(round(time.time() - start_time))
    gdn_logger.error(mess)
    native_logger.error(mess)
    return len(list_of_json_gdn), len(list_of_json_native)
    # return 1



def scraper_call(proxy_port):
    try:
        proxy = proxy_port[0]
        port = proxy_port[1]
        metric = Metrics(idx=proxy)
        start_http_server(int(port))


        gdn_logger = setup_logger('gdn_logger', "log/gdn_android_" + str(proxy) + ".log")
        gdn_ads_logger = setup_logger('gdn_ads_logger', "log/ads_gdn_android_" + str(proxy) + ".log")
        
        native_logger = setup_logger('native_logger', "log/native_android_" + str(proxy) + ".log")
        native_ads_logger = setup_logger('native_ads_logger', "log/ads_native_android_" + str(proxy) + ".log")
        
        # ios_agent = 'Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X; en-us) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B176 Safari/7534.48.3'
        android_agent = 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.91 Mobile Safari/537.36'
        #desktop_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'

        for i in range(3):
            try:
                driver, r_session, ip_dict = init.init_dc(proxy, metric, gdn_logger, native_logger, android_agent)
                break
            except:
                pass

        url_counter = 0
        uniq_ad_dict = dict()
        url_list = open("./input/gdn_urls.txt").read().splitlines()
        random.shuffle(url_list)

        while True:
            for r1_url in url_list:
                try:
                    r1_url= r1_url.replace("\n", "")
                    u = urlparse(r1_url)
                    base_url = u.scheme + "://" + u.netloc
                
                    url_counter += 1
                    offset = 100000
                    try:
                        ad_count_gdn, ad_count_native = main(driver, r_session, r1_url, base_url, ip_dict, gdn_logger, native_logger, offset, proxy, metric, uniq_ad_dict)
                                        
                        gdn_ads_logger.error(str(r1_url) + " : " + str(proxy) + " : " + str(ad_count_gdn))
                        native_ads_logger.error(str(r1_url) + " : " + str(proxy) + " : " + str(ad_count_native))
                        metric.processed_gdn_url.labels(country=proxy, status="pass", source="android").inc()
                        metric.processed_native_url.labels(country=proxy, status="pass", source="android").inc()
                    except Exception as e:
                        print("[GDN_Native Scraper failed] Proxy:", proxy, " Message:", e)
                        metric.processed_gdn_url.labels(country=proxy, status="fail", source="android").inc()
                        metric.processed_native_url.labels(country=proxy, status="fail", source="android").inc()

                    if url_counter > 20:
                        while True:
                            try:
                                url_counter = 0
                                print("Restarting chrome driver....")
                                try:
                                    driver.quit()
                                except:
                                    pass
                                driver, r_session, ip_dict = init.init_dc(proxy, metric, gdn_logger, native_logger, android_agent)
                                break
                            except:
                                pass
                except:
                    pass
    except:
        pass


def multi_chrome():
    logging.Formatter.converter = time.gmtime
    install_mp_handler()
    pool_list = [("us", "8000"), ("at", "8001"), ("au", "8002"), ("be", "8003"), ("br", "8004"), ("ca", "8005"),("ch", "8006"), ("cn", "8007")]
    p = Pool(8)
    p.map(scraper_call, pool_list)
    p.terminate()
    
    

if __name__ == "__main__":
    multi_chrome()
