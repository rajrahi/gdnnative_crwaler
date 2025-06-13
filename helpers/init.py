import time
import json
import random
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


def init_dc(proxy, metric, gdn_logger, native_logger, user_agent):
    try:
        opt = webdriver.ChromeOptions()
        opt.browser_version = "138"
        path_to_luminati = r"C://Users//Admin//Desktop//gdnnative_crwaler//helpers//luminati_new//" + str(proxy) 
        print(path_to_luminati)
        opt.add_argument("--load-extension="+path_to_luminati)
        prefs = {"profile.default_content_setting_values.notifications": 2,
                 "profile.default_content_setting_values.geolocation": 2, "download.default_directory": "NUL",
                 "download.prompt_for_download": False}


        opt.add_experimental_option("prefs", prefs)
        opt.add_argument(f'user-agent={user_agent}')
        opt.add_argument("start-maximized")
        opt.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        opt.add_argument("user-data-dir=C:/Users/Admin/AppData/Local/Google/Chrome/User Data/chrome_profile/" + proxy)
        caps = DesiredCapabilities.CHROME
        caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        driver = webdriver.Chrome(options=opt)# executable_path='./chromedriver.exe', desired_capabilities=caps)
        driver.set_page_load_timeout(120)
        p_proxy = str(proxy).split("_")[0]
        r_session = requests.Session()
        # p = 'http://lum-customer-empmonitor-zone-kushal_poweradspy-country-'+p_proxy+':rp5ldwq1inn4@brd.superproxy.io:33335'
        p = 'http://brd-customer-hl_79ebdb8c-zone-poweradspy_all_team_dc_ip-country-us:rp5ldwq1inn4@brd.superproxy.io:33335'
        # p = 'http://brd-customer-hl_79ebdb8c-zone-poweradspy_all_team_dc_ip-country-' + str(p_proxy) + ':rp5ldwq1inn4@brd.superproxy.io:33335'
        # http://brd-customer-hl_79ebdb8c-zone-poweradspy_all_team_dc_ip:rp5ldwq1inn4@brd.superproxy.io:33335
        # brd.superproxy.io:33335:brd-customer-hl_79ebdb8c-zone-poweradspy_all_team_dc_ip-ip-212.80.203.149:rp5ldwq1inn4
        print(p)

        proxies = {'https': p, }
        r_session.proxies = proxies
        r_session.headers.update({'user-agent': user_agent})
        
        try:
            driver.get("https://geolocation.poweradspy.com/")
            time.sleep(5)
            ip_text = driver.find_element(by="xpath", value="//body").text
            new_dict = json.loads(ip_text)
            print(new_dict)
            ip_dict = {'ipAddress': new_dict['ip'], 'countryName': new_dict['countryName'],
                       'stateProv': new_dict['regionName'], 'city': new_dict['cityName']}
            print(ip_dict)
            if "ipAddress" in ip_dict:
                return driver, r_session, ip_dict
            else:
                print('error while getting ip_dict from geolocation api')
        except:
            driver.get("https://api.db-ip.com/v2/free/self")
            text = driver.find_element(by="tag name", value="pre").text
            ip_dict = json.loads(text)
            print(ip_dict)  # for local testing
            if "ipAddress" in ip_dict:
                return driver, r_session, ip_dict
            driver.close()
            driver.quit()
            print('Error while getting ip_dict')
    except Exception as e:
        print("[INIT DC ERROR] for country " + proxy + " : " + str(e))
        gdn_logger.error("[INIT DC ERROR] for country " + proxy + " : " + str(e))
        native_logger.error("[INIT DC ERROR] for country " + proxy + " : " + str(e))
        driver.quit()


