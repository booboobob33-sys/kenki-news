
import base64
import requests

url = "https://news.google.com/rss/articles/CBMimAFBVV95cUxOYVdOSVE4SzRLMWpHXzBMdkZxVUt1Mkt0NV82M2pvTWRlTTdadEJUQ2RmTzNFUktKS1R6UWh4RkhGT1J6TldpX0ZDa2M0Tmxjd0xYZE5CMFJxRGZOOWxJLW0ta3p4RFBYeUF2VjN6QV9qLVUtZmVlYXMzaUViY2prQjlmZm1MZWJRUjUxaHhpc3Q2c0ZGTEtjTA?oc=5&hl=en-US&gl=US&ceid=US:en"

def decode_google(url):
    try:
        # Extract base64 part
        start = url.find("articles/") + 9
        end = url.find("?")
        if end == -1: end = len(url)
        b64 = url[start:end]
        
        # Add padding
        missing_padding = len(b64) % 4
        if missing_padding:
            b64 += '=' * (4 - missing_padding)
            
        decoded_bytes = base64.urlsafe_b64decode(b64)
        decoded_str = decoded_bytes.decode('latin1', errors='ignore') # Binary junk potential
        
        print("Decoded raw:", decoded_str)
        
        # Search for http
        import re
        urls = re.findall(r'(https?://[^\x00-\x1f\x7f-\xff]+)', decoded_str)
        print("Found URLs:", urls)
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    decode_google(url)
