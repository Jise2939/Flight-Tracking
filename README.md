# 美以伊戰事下的中東航空停擺

**數據新聞專題 | 地緣政治衝突如何衝擊全球航空**

🔗 **線上閱讀**：[https://jise2939.github.io/Flight-Tracking/](https://jise2939.github.io/Flight-Tracking/)

---

## 主要目的

- 將航班運行資料收集並以可視化形式呈現給公眾與研究者。 
- 對比戰前/戰後的取消率與取消班次，說明衝突對客運與貨運供應鏈的衝擊。
- 提供一個可復現的小型數據集供後續分析或教學使用。

---

## 資料說明（Dataset）

- 目標機場：DXB（迪拜）、DOH（多哈）、AUH（阿布扎比）、JED（吉達）、RUH（利雅得）
- 日期範圍：2026-02-26 至 2026-03-09
- 格式：JSON（每次抓取一個 JSON），字段包含：total（計劃班次）、cancelled（已取消）、rate_pct（取消率）、timestamp 等。

---

## 文件說明

以下為倉庫中主要檔案與資料夾的簡要說明：

- `index.html` — 專題主頁（前端展示與可視化）。
- `README.md` — 本文件（文件說明）。
- `output/` — 原始抓取的 raw JSON 資料集。
- `中東地圖.png` — 地圖截圖素材，用於頁面展示。 
- `airindex.webp` — 貨運費率走勢圖（展示用資源）。
- `.env.example` — 環境變數示例。
- `.gitignore` — 忽略規則。
- `crawl_cancel_rates.py` — 爬蟲腳本，用於抓取航班取消數據。

---


## 免責聲明

本文僅做資訊傳播用途，不構成商業建議。數據存在一定誤差，僅供參考。

---

**Contributor：@[Jise2939](https://github.com/Jise2939) @[TianRuowen](https://github.com/TianRuowen) @[NikkoNiJincheng](https://github.com/njc532313-dev) | 2026 年 3 月 10 日**