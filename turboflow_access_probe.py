import requests,re,json,os
S=requests.Session(); S.headers.update({"User-Agent":"Mozilla/5.0"})
urls=[
 "https://turboflow.gitbook.io/whitepaper/turboflow/professional-access/api-trading.md",
 "https://turboflow.gitbook.io/whitepaper/turboflow/turboflow-core/execution-layer.md",
 "https://turboflow.gitbook.io/whitepaper/turboflow/turboflow-core/parameters-and-risk-logic.md",
 "https://turboflow.gitbook.io/whitepaper/turboflow/resources/tutorial.md"
]
for u in urls:
 r=S.get(u,timeout=30); print("\n=====URL",u,"STATUS",r.status_code,"=====\n"); print(r.text[:100000])
