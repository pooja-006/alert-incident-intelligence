import json
import xml.etree.ElementTree as ET
import pandas as pd
import re

alerts = []

# -----------------------------
# 1. Parse Meraki JSON
# -----------------------------
try:
    with open("data/meraki.json", "r", encoding="utf-8") as f:
        meraki_data = json.load(f)

    for alert in meraki_data:
        alerts.append({
            "source": "meraki",
            "organization": alert.get("organizationName"),
            "device": alert.get("deviceName"),
            "alert_type": alert.get("alertType"),
            "severity": alert.get("alertLevel"),
            "timestamp": alert.get("occurredAt")
        })

    print("Meraki alerts loaded")

except Exception as e:
    print("Meraki parsing error:", e)


# -----------------------------
# 2. Parse Auvik JSON
# -----------------------------
try:
    with open("data/auvik.json", "r", encoding="utf-8") as f:
        auvik_data = json.load(f)

    for alert in auvik_data:
        alerts.append({
            "source": "auvik",
            "organization": alert.get("companyName"),
            "device": alert.get("entityName"),
            "alert_type": alert.get("alertName"),
            "severity": alert.get("alertSeverityString"),
            "timestamp": alert.get("date")
        })

    print("Auvik alerts loaded")

except Exception as e:
    print("Auvik parsing error:", e)


# -----------------------------
# 3. Parse N-Central XML
# -----------------------------
try:
    with open("data/ncentral.xml", "r", encoding="utf-8") as f:
        xml_data = f.read()

    # Remove repeated XML headers
    xml_data = re.sub(r'<\?xml.*?\?>', '', xml_data)

    # Add a root tag
    xml_data = "<root>" + xml_data + "</root>"

    root = ET.fromstring(xml_data)

    for n in root.findall("notification"):
        alerts.append({
            "source": "ncentral",
            "organization": n.findtext("CustomerName"),
            "device": n.findtext("DeviceName"),
            "alert_type": n.findtext("AffectedService"),
            "severity": n.findtext("QualitativeNewState"),
            "timestamp": n.findtext("TimeOfStateChange")
        })

    print("N-Central alerts loaded")

except Exception as e:
    print("N-Central parsing error:", e)


# -----------------------------
# 4. Convert to DataFrame
# -----------------------------
df = pd.DataFrame(alerts)

print("\nSample Data:")
print(df.head())


# -----------------------------
# 5. Save Combined Dataset
# -----------------------------
df.to_csv("stitched_alerts.csv", index=False)

print("\nData stitching complete!")
print("File saved as stitched_alerts.csv")