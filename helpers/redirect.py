import json
import tldextract
from urllib.request import urlparse, urljoin

def is_validurl(url):
	parsed = urlparse(url)
	return bool(parsed.netloc) and bool(parsed.scheme)

def get(driver, r_session, j): #Returns both html and http redirects
	unique_redirects = dict()
	url1 = j["redirect_url"]
	url2 = j["destination_url"]
	destination = tldextract.extract(url2)
	unique_redirects[url1]= ""
	try:
		redirect_list = []
		for entry in driver.get_log('performance'):
			dict1 = json.loads(entry['message'])['message']
			if dict1['method'] == 'Network.requestWillBeSent':
				redirect_list.append(dict1["params"]['documentURL'])
		flag = 0
		for x in redirect_list:
			if tldextract.extract(x).domain == tldextract.extract(url1).domain:
				flag = 1
			if tldextract.extract(x).domain == tldextract.extract(url2).domain:
				flag = 0
			if flag == 1 and tldextract.extract(x).domain not in destination.domain and is_validurl(x) and not (any([x1 for x1 in list(unique_redirects.keys()) if tldextract.extract(x).domain in tldextract.extract(x1).domain ])):
				unique_redirects[x] = ""
	except Exception as e:
		raise Exception ("JS Redirect error: ", e)
	try:
		#base = tldextract.extract(url1)
		res = r_session.get(url1, timeout=60)
		if len(res.history) > 0:
			for x in res.history:
				redirect = tldextract.extract(x.url)
				if is_validurl(x.url) and redirect.domain not in destination.domain and not (any([x1 for x1 in list(unique_redirects.keys()) if tldextract.extract(x.url).domain in tldextract.extract(x1).domain ])):
					unique_redirects[x.url]=''
			#print(unique_redirects)
			return list(unique_redirects.keys())
		else:
			return [url1]
	except Exception as e:
		raise Exception ("HTTP Redirect error: ", e)