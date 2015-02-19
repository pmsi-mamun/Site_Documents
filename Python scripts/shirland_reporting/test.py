__author__ = 'mrashid'

import hashlib, re

def call_me():
    text ='weight(zip:14005 in 14055) [PerFieldSimilarity], result of:\' (86968448)'
    p = 'weight\((\\w+):'
    cltext = cleanup_text_test(text, p)
    print(cltext)

def cleanup_text_test(text, p):
    cleaned = "".join(re.findall(p, text))
    return cleaned



call_me()