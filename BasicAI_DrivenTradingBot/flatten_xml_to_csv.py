import xml.etree.ElementTree as ET
import csv
import sys

def clean_text(text):
    """Remove unwanted control/non-UTF8 characters."""
    if text is None:
        return ""
    import re
    return re.sub(r'[^\x20-\x7E]+', '', text)

def parse_large_xml(input_file, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        # Define the CSV header
        fieldnames = [
            'order_id',
            'customer_name',
            'customer_email',
            'notes',
            'item_number',
            'product',
            'qty'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Set up streaming XML parsing
        context = ET.iterparse(input_file, events=('end',))
        for event, elem in context:
            if elem.tag == 'order':
                # Extract order-level fields
                order_id = elem.findtext('id', default='').strip()
                customer_elem = elem.find('customer')
                customer_name = customer_elem.findtext('name', default='').strip() if customer_elem is not None else ''
                customer_email = customer_elem.findtext('email', default='').strip() if customer_elem is not None else ''
                notes = clean_text(elem.findtext('notes', default='').strip())

                # Extract and flatten items
                items_elem = elem.find('items')
                if items_elem is not None:
                    for idx, item_elem in enumerate(items_elem.findall('item'), start=1):
                        product = item_elem.findtext('product', default='').strip()
                        qty = item_elem.findtext('qty', default='').strip()
                        writer.writerow({
                            'order_id': order_id,
                            'customer_name': customer_name,
                            'customer_email': customer_email,
                            'notes': notes,
                            'item_number': idx,
                            'product': product,
                            'qty': qty
                        })
                else:
                    # Write row even if there are no items (optional)
                    writer.writerow({
                        'order_id': order_id,
                        'customer_name': customer_name,
                        'customer_email': customer_email,
                        'notes': notes,
                        'item_number': '',
                        'product': '',
                        'qty': ''
                    })
                # Free memory
                elem.clear()

if __name__ == '__main__':
    # Usage: python script.py input.xml output.csv
    if len(sys.argv) != 3:
        print("Usage: python script.py input.xml output.csv")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    parse_large_xml(input_file, output_file)
    print(f"Done! Output written to {output_file}")
