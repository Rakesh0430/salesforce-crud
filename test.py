import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import random
import string

def generate_record(i):
    """
    Generate a single record with fields formatted as follows:
    <Record>
        <Id>001TEST000001</Id>
        <Name>Test Account 1</Name>
        <AccountNumber>Acc-8032</AccountNumber>
        <Site>Houston</Site>
        <Type>Reseller</Type>
        <Industry>Technology</Industry>
        <AnnualRevenue>64180577</AnnualRevenue>
        <Rating>Hot</Rating>
        <Phone>421-116-9928</Phone>
        <Fax>456-923-7099</Fax>
        <Website>www.test1.com</Website>
        <TickerSymbol>OAO</TickerSymbol>
        <Ownership>Subsidiary</Ownership>
        <NumberOfEmployees>6060</NumberOfEmployees>
    </Record>

    The values are generated randomly for demonstration purposes.
    """
    record = {
        "Id": f"001TEST{i:06d}",
        "Name": f"Test Account {i}",
        "AccountNumber": f"Acc-{random.randint(1000, 9999)}",
        "Site": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        "Type": random.choice(["Customer", "Partner", "Reseller", "Prospect"]),
        "Industry": random.choice(["Healthcare", "Technology", "Finance", "Retail", "Manufacturing"]),
        "AnnualRevenue": random.randint(1_000_000, 100_000_000),
        "Rating": random.choice(["Hot", "Warm", "Cold"]),
        "Phone": f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        "Fax": f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        "Website": f"www.test{i}.com",
        "TickerSymbol": ''.join(random.choices(string.ascii_uppercase, k=3)),
        "Ownership": random.choice(["Private", "Public", "Subsidiary"]),
        "NumberOfEmployees": random.randint(10, 10000)
    }
    return record

def prettify(elem):
    """
    Return a pretty-printed XML string for the Element.
    This function uses minidom to add indentation and a proper XML declaration.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="    ")

def main():
    filename = "test_records_2000.xml"
    total_records = 2000

    # Create the root element
    root = ET.Element("Records")

    # Generate each record and add it to the root
    for i in range(1, total_records + 1):
        rec_data = generate_record(i)
        record_elem = ET.SubElement(root, "Record")
        for key, value in rec_data.items():
            child = ET.SubElement(record_elem, key)
            child.text = str(value)

    # Get a pretty-printed XML string with an XML declaration.
    pretty_xml = prettify(root)

    # Write the pretty-printed XML to a file.
    with open(filename, "w", encoding="utf-8") as xml_file:
        xml_file.write(pretty_xml)

    print(f"XML file '{filename}' with {total_records} records created successfully.")

if __name__ == "__main__":
    main()