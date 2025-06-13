var config = {
    mode: "fixed_servers",
    rules: {
      singleProxy: {
        scheme: "http",
        host: "brd.superproxy.io",
        port: parseInt(22225)
      },
      bypassList: [""]
    }
  };

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
username: "brd-customer-hl_79ebdb8c-zone-poweradspy_all_team_dc_ip-country-ua",
password: "rp5ldwq1inn4"        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        {urls: ["<all_urls>"]},
        ['blocking']
);