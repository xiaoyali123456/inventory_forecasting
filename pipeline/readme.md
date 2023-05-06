# LLD

[![](https://mermaid.ink/img/pako:eNp9UstuwjAQ_BXLJ1cCPoBDpVIoPbSXtsdI1mIvxGr8qL2GIsS_10kUFCrBzTP7mNldn7jyGvmc7yKEmr19VI6xlDc9DDFErzClWarbAGPaRFRkvGNfi555ElskVcuIPxkTpVk4PrDp9JEthKpRfUuHB2mhpLSRtgadvlLZ-tITEt0ReRZQiD3KnDBKpEaC0zJE1EbRRXFZrADliIWZsN_dRtoyXNMh4_boyMfjUFXa3zKUwIbGuN0dQytRTFyEXwQeLFzgWihvN8bhLYHgE_3f7KsYs9eF4zt0CqOVdXjkuMPXAnzCLUYLRpdDn9qGFacaLVZ8Xp4at5AbqnjlziU1Bw2EK23KtvicYsYJh0z-8-jUgPucpYEyjx1I7Ere-__UfavzH4Xpz60?type=png)](https://mermaid-js.github.io/mermaid-live-editor/edit#pako:eNp9UstuwjAQ_BXLJ1cCPoBDpVIoPbSXtsdI1mIvxGr8qL2GIsS_10kUFCrBzTP7mNldn7jyGvmc7yKEmr19VI6xlDc9DDFErzClWarbAGPaRFRkvGNfi555ElskVcuIPxkTpVk4PrDp9JEthKpRfUuHB2mhpLSRtgadvlLZ-tITEt0ReRZQiD3KnDBKpEaC0zJE1EbRRXFZrADliIWZsN_dRtoyXNMh4_boyMfjUFXa3zKUwIbGuN0dQytRTFyEXwQeLFzgWihvN8bhLYHgE_3f7KsYs9eF4zt0CqOVdXjkuMPXAnzCLUYLRpdDn9qGFacaLVZ8Xp4at5AbqnjlziU1Bw2EK23KtvicYsYJh0z-8-jUgPucpYEyjx1I7Ere-__UfavzH4Xpz60)

# Source Code of the Diagram

```mermaid
graph LR
  subgraph prprocess.sh
    direction TB
    A(fetch_requests.py) --> B(check_new_match.py)
  end
  subgraph forecast.sh
    direction TB
    C(active_user_etl_and_predict.py) --> D(feature.py, xgb_model.py, inventory_prediction.py)
  end
  subgraph sampling.sh
    direction TB
    E(etl.py) --> F(ewma.py) --> G(combine.py)
  end
  subgraph postprocess.sh
    H(postprocess.py)
  end
  prprocess.sh --> forecast.sh --> sampling.sh --> postprocess.sh
```