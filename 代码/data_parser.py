from bs4 import BeautifulSoup
import jieba

def clean_text(text):
    return ''.join([c if c.isalnum() else ' ' for c in text]).strip()

def parse_html(html):
    soup = BeautifulSoup(html, 'lxml')
    text = soup.get_text()
    cleaned = clean_text(text)
    words = jieba.lcut(cleaned)
    return {
        "text": cleaned,
        "keywords": list(set(words))
    }
