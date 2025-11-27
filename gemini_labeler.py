#!/usr/bin/env python3
# gemini_labeler.py
# Traffy Fondue Gemini Labeler using google-genai v1.52.0

import argparse
import asyncio
import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm
import base64
from google import genai
import json

###############################################################################
# ARGUMENTS
###############################################################################
parser = argparse.ArgumentParser(description="Traffy Fondue Gemini Labeler Script")
parser.add_argument("--input", type=str, required=True, help="Input CSV file")
parser.add_argument("--output", type=str, required=True, help="Output CSV file")
parser.add_argument("--debug", type=str, default="debug.txt", help="Debug output file for JSON responses")
parser.add_argument("--api_key", type=str, required=True, help="Gemini API key")
parser.add_argument("--model", type=str, default="gemini-2.5-flash-lite", help="Gemini chat model name")
parser.add_argument("--concurrency", type=int, default=16)
parser.add_argument("--batch_size", type=int, default=20)
parser.add_argument("--timeout", type=int, default=60)
args = parser.parse_args()

###############################################################################
# CLIENT SETUP
###############################################################################
client = genai.Client(api_key=args.api_key)
print(f"✅ Using model: {args.model}")

###############################################################################
# SYSTEM PROMPT
###############################################################################
SYSTEM_PROMPT = """คุณเป็นผู้ช่วยในการประเมินคุณภาพของเรื่องร้องเรียนจากประชาชนในกรุงเทพมหานคร ที่จะส่งไปยังส่วนบริหารงานกรุงเทพมหานคร

วัตถุประสงค์: ประเมินว่าเรื่องร้องเรียนมีข้อมูลเพียงพอสำหรับเจ้าหน้าที่ส่วนบริหารงานกรุงเทพมหานครในการดำเนินการแก้ไขปัญหาหรือไม่

กระบวนการคิด (Chain of Thought):
1. วิเคราะห์ประเภทของปัญหา (type) ที่ประชาชนร้องเรียน
2. ตรวจสอบว่ามีรายละเอียดที่จำเป็นสำหรับประเภทปัญหานั้นหรือไม่:
   - สถานที่เกิดเหตุ (ชื่อถนน, ซอย, แยก, หมายเลขบ้าน, พิกัด)
   - ลักษณะอาการของปัญหาที่ชัดเจน
   - ความรุนแรง หรือขนาดของปัญหา (ถ้าเกี่ยวข้อง)
3. ตรวจสอบรูปภาพ (ถ้ามี):
   - รูปภาพแสดงปัญหาได้ชัดเจนหรือไม่
   - รูปภาพช่วยเสริมข้อมูลที่ขาดหายในข้อความหรือไม่
4. สรุปผล: เจ้าหน้าที่สามารถระบุตำแหน่งและดำเนินการแก้ไขได้หรือไม่

เกณฑ์การตัดสิน:
- "1" = มีข้อมูลเพียงพอ (sufficient) ได้แก่:
  * มีสถานที่ชัดเจน (ชื่อถนน/ซอย/แยก หรือพิกัด หรือจุดสังเกต) 
  * คำอธิบายปัญหาชัดเจน หรือรูปภาพแสดงปัญหาได้ชัดเจน
  * เจ้าหน้าที่สามารถไปถึงจุดเกิดเหตุและดำเนินการได้

- "0" = ข้อมูลไม่เพียงพอ (not sufficient) ได้แก่:
  * ไม่มีสถานที่ หรือระบุสถานที่กว้างเกินไป (เช่น "กรุงเทพ", "ถนนสุขุมวิท" โดยไม่ระบุช่วงหรือซอย)
  * คำอธิบายคลุมเครือ ไม่ชัดเจนว่าปัญหาคืออะไร
  * รูปภาพไม่ชัด หรือไม่เกี่ยวข้องกับปัญหา
  * ข้อมูลไม่เพียงพอให้เจ้าหน้าที่สามารถดำเนินการได้

- "2" = ไม่แน่ใจ แต่มีหลักฐานบางส่วน (uncertain but has evidence)
  * ข้อมูลเริ่มต้นไม่ชัดเจนพอที่จะมั่นใจว่าสามารถแก้ไขได้
  * แต่มี photo_after ที่มีความเกี่ยวข้องบางส่วนกับประเภทปัญหา
  * แต่ไม่สามารถอธิบายได้ชัดเจนว่า photo_after แสดงผลจากการแก้ไขอย่างไร
  * ไม่สามารถยืนยันได้ว่า photo_after เป็นผลจากการดำเนินการที่คุณจินตนาการไว้

สำหรับแต่ละเรื่องร้องเรียน:
1. วิเคราะห์ทีละขั้นตอนตามกระบวนการด้านบน
2. ตอบด้วย JSON object ที่มี:
   - "id": Ticket ID ของเรื่องร้องเรียน (ใช้ค่าที่ระบุไว้ใน Ticket ID)
   - "reasoning": การวิเคราะห์โดยย่อ (อธิบายการคิดของคุณในฐานะเจ้าหน้าที่)
   - "label": ตัวเลข 0, 1, หรือ 2

ตัวอย่างรูปแบบการตอบ:
```json
[
  {"id": "ABC123", "reasoning": "มีชื่อถนนและจุดสังเกตชัดเจน ฉันสามารถไปที่นั่นและเติมหลุมได้ รูป photo_after แสดงว่าหลุมถูกเติมจริง", "label": 1},
  {"id": "XYZ789", "reasoning": "ไม่ระบุสถานที่ชัดเจน เพียงแค่บอกว่า 'ในกรุงเทพ' ฉันไม่รู้ว่าจะไปที่ไหน", "label": 0},
  {"id": "DEF456", "reasoning": "ร้องเรียนน้ำท่วมแต่ไม่ระบุตำแหน่งชัดเจน มี photo_after แสดงถนนแห้ง แต่ไม่แน่ใจว่าเป็นสถานที่เดียวกันหรือแก้ไขอย่างไร", "label": 2}
]
```

ตอบเป็น JSON array เท่านั้น ไม่ต้องมีข้อความอื่น"""

###############################################################################
# IMAGE FETCH
###############################################################################
async def fetch_image_bytes(session, url, timeout=10):
    if not url or not isinstance(url, str):
        return None
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status != 200:
                return None
            return await resp.read()
    except Exception:
        return None

###############################################################################
# CLASSIFY BATCH
###############################################################################
async def classify_batch(session, batch_df, batch_num, debug_file):
    """Process a batch of rows in a single API call"""
    
    # Build content parts for the batch
    content_parts = [{"text": SYSTEM_PROMPT + "\n\nประเมินเรื่องร้องเรียนต่อไปนี้:\n\n"}]
    
    # Keep track of indices and ticket_ids
    batch_indices = []
    batch_ticket_ids = []
    
    for idx, (_, row) in enumerate(batch_df.iterrows(), 1):
        batch_indices.append(idx)
        ticket_id = row.get("ticket_id", f"unknown_{idx}")
        batch_ticket_ids.append(ticket_id)
        
        # Add separator
        content_parts.append({"text": f"--- เรื่องร้องเรียนที่ {idx} (Ticket ID: {ticket_id}) ---\n"})
        
        # Add complaint type
        complaint_type = row.get("type", "ไม่ระบุ")
        if pd.notna(complaint_type):
            content_parts.append({"text": f"ประเภทปัญหา: {complaint_type}\n"})
        
        # Add organization
        organization = row.get("organization", "")
        if pd.notna(organization):
            content_parts.append({"text": f"หน่วยงานรับผิดชอบ: {organization}\n"})
        
        # Add address
        address = row.get("address", "")
        if pd.notna(address) and str(address).strip():
            content_parts.append({"text": f"ที่อยู่/สถานที่: {address}\n"})
        
        # Add text content
        comment = row.get("comment", "")
        if pd.notna(comment):
            content_parts.append({"text": f"รายละเอียด: {comment}\n"})
        
        # Add image if available
        photo_url = row.get("photo", "")
        if pd.notna(photo_url):
            img_bytes = await fetch_image_bytes(session, photo_url, args.timeout)
            if img_bytes:
                img_b64 = base64.b64encode(img_bytes).decode("utf-8")
                content_parts.append({
                    "inline_data": {
                        "mime_type": "image/jpeg",
                        "data": img_b64
                    }
                })
                content_parts.append({"text": " (รูปก่อนแก้ไข)\n"})
        
        # Add outcome data
        outcome_parts = []
        
        # State
        state = row.get("state", "")
        if pd.notna(state):
            outcome_parts.append(f"สถานะ: {state}")
        
        # Photo after
        photo_after = row.get("photo_after", "")
        has_photo_after = pd.notna(photo_after) and str(photo_after).strip() != ""
        outcome_parts.append(f"มีรูปหลังแก้ไข: {'ใช่' if has_photo_after else 'ไม่'}")
        
        # Star rating
        star = row.get("star", "")
        if pd.notna(star):
            try:
                star_val = float(star)
                outcome_parts.append(f"คะแนนความพึงพอใจ: {star_val}/5")
            except:
                pass
        
        if outcome_parts:
            content_parts.append({"text": f"ข้อมูล Outcome: {', '.join(outcome_parts)}\n"})
        
        content_parts.append({"text": "\n"})

    try:
        # Call API with entire batch
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=args.model,
            contents=content_parts,
            config=genai.types.GenerateContentConfig(
                temperature=0,
            )
        )

        text = response.text.strip()
        
        # Write raw response to debug file
        with open(debug_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"BATCH {batch_num}\n")
            f.write(f"{'='*80}\n")
            f.write(text)
            f.write(f"\n{'='*80}\n\n")
        
        # Try to parse JSON response
        # Remove markdown code blocks if present
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        
        try:
            results = json.loads(text)
            
            # Create a mapping from ticket_id to label
            label_map = {}
            for item in results:
                if isinstance(item, dict) and "id" in item and "label" in item:
                    label_str = str(item["label"])
                    # Validate label is 0, 1, or 2
                    if label_str in ["0", "1", "2"]:
                        label_map[str(item["id"])] = label_str
                    else:
                        label_map[str(item["id"])] = "254"  # Invalid label from LLM
            
            # Return labels in order, matching by ticket_id
            batch_results = []
            for ticket_id in batch_ticket_ids:
                ticket_id_str = str(ticket_id)
                if ticket_id_str in label_map:
                    batch_results.append(label_map[ticket_id_str])
                else:
                    batch_results.append("253")  # Missing result in JSON
            
            return batch_results
            
        except json.JSONDecodeError:
            # If JSON parsing fails, return 253 for all
            return ["253"] * len(batch_indices)

    except Exception as e:
        # Error occurred, return 255 for all in batch
        return ["255"] * len(batch_df)

###############################################################################
# CLASSIFY ALL
###############################################################################
async def classify_all(df, concurrency=16, batch_size=20, debug_file="debug.txt"):
    sem = asyncio.Semaphore(concurrency)
    
    # Clear debug file
    with open(debug_file, 'w', encoding='utf-8') as f:
        f.write("GEMINI API DEBUG LOG\n")
        f.write(f"Generated: {pd.Timestamp.now()}\n")
        f.write("="*80 + "\n\n")
    
    # Split dataframe into batches
    batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
    
    async with aiohttp.ClientSession() as session:
        async def process_batch(batch_df, batch_num):
            async with sem:
                results = await classify_batch(session, batch_df, batch_num, debug_file)
                return (batch_num, results)  # Return batch number with results

        tasks = [process_batch(batch, i+1) for i, batch in enumerate(batches)]
        
        # Collect results with their batch numbers
        batch_results_dict = {}
        for coro in tqdm.as_completed(tasks, total=len(tasks), desc="Processing batches"):
            batch_num, batch_results = await coro
            batch_results_dict[batch_num] = batch_results
    
    # Reconstruct results in the correct order
    results = []
    for batch_num in sorted(batch_results_dict.keys()):
        results.extend(batch_results_dict[batch_num])
    
    return results

###############################################################################
# MAIN
###############################################################################
def main():
    print("Loading dataset:", args.input)
    df = pd.read_csv(args.input)
    
    # Check if 'ticket_id' column exists
    if 'ticket_id' not in df.columns:
        print("Warning: 'ticket_id' column not found. Will generate placeholder IDs.")
        df['ticket_id'] = [f"ticket_{i}" for i in range(len(df))]
    
    # Check if 'type' column exists
    if 'type' not in df.columns:
        print("Warning: 'type' column not found. Will use 'ไม่ระบุ' as default.")
        df['type'] = 'ไม่ระบุ'
    
    df = df[(df["comment"].notna()) | (df["photo"].notna())].reset_index(drop=True)
    print(f"Samples to label: {len(df)}")
    print(f"Batch size: {args.batch_size} (API batching enabled)")
    print(f"Debug output: {args.debug}")

    print("Running LLM classification...")
    labels = asyncio.run(classify_all(df, concurrency=args.concurrency, batch_size=args.batch_size, debug_file=args.debug))
    df["llm_label"] = labels

    print("Saving output to:", args.output)
    df.to_csv(args.output, index=False)
    
    # Print summary statistics
    label_counts = df["llm_label"].value_counts()
    print("\nResults Summary:")
    for label, count in label_counts.items():
        percentage = (count / len(df)) * 100
        label_desc = {
            "0": "Not Sufficient",
            "1": "Sufficient", 
            "2": "Uncertain (has partial evidence)",
            "253": "Missing in JSON Response",
            "254": "Invalid Label from LLM",
            "255": "API Error"
        }.get(label, "Unknown")
        print(f"   {label} ({label_desc}): {count} ({percentage:.1f}%)")
    
    print("Done!")

if __name__ == "__main__":
    main()