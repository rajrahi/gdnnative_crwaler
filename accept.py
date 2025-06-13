from selenium import webdriver
import requests
import json
import re
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def accept_cookie(driver, base_url):
    if "www.dailymail.co.uk" in base_url:

        try:
            popup_top = driver.find_elements(by = "class name", value = "mol-ads-cmp--btn-primary")
            popup_top.click()
        except:
            pass
        try:
            time.sleep(8)
            popup_top = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "closeButton")))
            popup_top.click()

        except:
            pass
    if "gulfnews.com" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "close-btn")
            popup_top.click()
        except:
            pass
    if "www.aktuality.sk" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "gdbutton")
            popup_top.click()
        except:
            pass
    if "www.independent.ie" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "qc-cmp-button")
            popup_top[-1].click()
        except:
            pass
    if "www.ndtv.com" in base_url:
        try:
            popup_top = WebDriverWait(driver, 5).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "notnow")))
            #popup_top = driver.find_elements_by_class_name("notnow")
            popup_top[-1].click()
        except:
            pass
    if "www.nettavisen.no" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "lp_privacy_ok")
            popup_top[-1].click()
        except:
            pass
    if "news.com.au" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "vms-sticky-close-button")
            popup_top[0].click()
        except:
            pass
    if "krone.at" in base_url:
        try:
            popup_top = driver.find_elements(By.ID, "krn_select_all")
            popup_top[0].click()
        except:
            pass
    if "www.index.hr" in base_url or "www.24sata.hr" in base_url or "www.vecernji.hr" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "didomi-components-button")
            popup_top[1].click()
        except:
            pass
    if "www.yahoo.com" in base_url:
        try:
            popup_top = driver.find_elements(By.ID, "mega-banner-close")
            popup_top[0].click()
        except:
            pass

    if "ekstrabladet.dk" in base_url or "politiken.dk" in base_url or "www.theintelligence.de" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "qc-cmp-button")
            popup_top[0].click()
        except:
            pass

    if "www.bt.dk" in base_url or "www.berlingske.dk" in base_url:
        try:
            popup_top = driver.find_elements(By.ID, "CybotCookiebotDialogBodyButtonAccept")
            popup_top[0].click()
        except:
            pass

    if "www.afterellen.com" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "cnaccept")
            popup_top[0].click()
        except:
            pass

    if "www.newsbomb.gr" in base_url:
        try:
            popup_top = driver.find_elements(By.ID, "onesignal-popover-cancel-button")
            popup_top[0].click()
        except:
            pass
    if "www.theintelligence.de" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "qc-cmp-button")
            popup_top[1].click()
        except:
            pass
    if "gameranx.com" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "tbl-next-up-closeBtn")
            popup_top[0].click()
        except:
            pass
    if "www.news-on-tour.de" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "_brlbs-btn")
            popup_top[0].click()
        except:
            pass
    if "toofab.com" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "optanon-allow-all")
            popup_top[0].click()
        except:
            pass
    if "www.mynet.com" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "banner_continue--2NyXA")
            popup_top[0].click()
        except:
            pass
        try:
            popup_top = driver.find_elements(By.ID, "privacy-close")
            popup_top[0].click()
        except:
            pass

    if "ireland-calling.com" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = "cc_btn")
            popup_top[0].click()
        except:
            pass
    if "www.terra.com.br" in base_url:
        try:
            popup_top = driver.find_elements(By.ID, "notification-deny")
            popup_top[0].click()
            time.sleep(1)
        except:
            pass
    if "www.newsit.gr" in base_url:
        try:
            popup_top = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[@id='qcCmpButtons']/button[2]")))
            popup_top.click()
        except:
            pass

    # New Updation...............

    if "https://www.repubblica.it" in base_url:
        try:
            popup_top = driver.find_element(By.XPATH, '//*[@id="iubenda-cs-banner"]/div/div/div/div[1]/div[2]/div/button[2]')
            popup_top.click()
        except:
            pass
            
    elif "https://www.express.co.uk" in base_url:
        try:
            popup_top = driver.find_elements(By.XPATH, '//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]')
            popup_top[0].click()
        except:
            pass
            
    elif "https://www.lefigaro.fr" in base_url:
        try:
            iframe = driver.find_element(By.XPATH, '//*[@id="appconsent"]/iframe')
            wait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(iframe))
            popup_top = driver.find_elements(By.XPATH, '/html/body/div/div/div/div/div/div/div[2]/aside/section/button[1]')
            popup_top[0].click()
        except:
            pass
            
    elif "https://www.onet.pl" in base_url:
        try:
            popup_top = driver.find_elements(By.XPATH, '//*[@id="rasp_cmp"]/div/div[6]/button[2]')
            popup_top[-1].click()
        except:
            pass
            
    elif "www.dailymail.co.uk" in base_url:
        try:
            popup_top = driver.find_element(By.ID, "closeButton")
            popup_top.click()
        except:
            pass
    elif "gulfnews.com" in base_url:
        try:
            popup_top = driver.find_element(By.ID,"onetrust-accept-btn-handler")
            popup_top.click()
           
        except:
            pass
    elif "www.aktuality.sk" in base_url:
        try:
            popup_top = driver.find_element(by = "class name", value = "gdbutton")
            popup_top.click()
        except:
            pass
            
    elif "https://www.sudinfo.be" in base_url or "https://www.elmundo.es" in base_url or "https://www.krone.at" in base_url or "https://www.lavanguardia.com" in base_url or "https://www.jutarnji.hr" in base_url or "www.independent.ie" in base_url or "www.index.hr" in base_url or "www.24sata.hr" in base_url or "www.vecernji.hr" in base_url:
        try:
            popup_top = driver.find_element(By.ID, "didomi-notice-agree-button")
            popup_top.click()
            
        except:
            pass
    elif "www.ndtv.com" in base_url:
        try:
            popup_top = driver.find_element(by = "class name", value = 'allow')
            popup_top.click()
        except:
            pass
    elif "www.nettavisen.no" in base_url:
        try:
            popup_top = driver.find_elements(by = "class name", value = 'lp_privacy_ok')
            popup_top[0].click()
        except:
            pass
    elif "news.com.au" in base_url:
        try:
            popup_top = driver.find_elements(by = 'class name', value="vms-sticky-close-button")
            popup_top[0].click()
        except:
            pass
    elif "https://www.heise.de" in base_url:
        try:
            iframe = driver.find_element(By.XPATH, '//*[@id="sp_message_iframe_795002"]')
            wait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(iframe))
            popup_top = driver.find_elements(by = "tag name", value = 'button')
            popup_top[-2].click()
        except:
            pass
    elif "www.yahoo.com" in base_url:
        try:
            popup_top = driver.find_elements(By.ID, "mega-banner-close")
            popup_top[0].click()
        except:
            pass

    elif "ekstrabladet.dk" in base_url or "politiken.dk" in base_url or "www.bt.dk" in base_url:
        try:
            popup_top = driver.find_element(By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
            popup_top.click()
        except:
            pass
    elif "https://www.newsit.gr" in base_url:
        try:
            popup_top = driver.find_element(By.XPATH, '//*[@id="qc-cmp2-ui"]/div[2]/div/button[3]')
            popup_top.click()
        except:
            pass
    elif "https://www.bild.de" in base_url:
        try:
            iframe = driver.find_element(By.XPATH, '//iframe[@id="sp_message_iframe_772534"]')
            wait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(iframe))
            popup_top = driver.find_element(By.XPATH, '//*[@id="notice"]/div[3]/div[2]/div[2]/button')
            popup_top.click()
        except:
            pass