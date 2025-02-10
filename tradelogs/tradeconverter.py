import csv
from datetime import datetime

def convert_tlg_to_csv(input_file, output_file):
    with open(input_file, 'r') as tlg_file, open(output_file, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        # Write CSV header
        csv_writer.writerow(['Date', 'Time', 'Symbol', 'Quantity', 'Price', 'Side'])
        
        for line in tlg_file:
            if not line.startswith('STK_TRD'):
                continue

            fields = line.strip().split('|')
            
            try:
                # Extract fields from pipe-delimited format
                symbol = fields[2]
                action = fields[5].upper()
                date_str = fields[7]  # YYYYMMDD format
                time_str = fields[8]
                quantity = abs(float(fields[10]))  # Use absolute value
                price = fields[12]  # Correct price field position

                # Convert date format
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                formatted_date = date_obj.strftime('%m/%d/%Y')

                # Determine buy/sell side based on action type
                if 'SELL' in action:
                    side = 'Sell'
                elif 'BUY' in action:
                    side = 'Buy'
                else:
                    side = 'Unknown'

                csv_writer.writerow([
                    formatted_date,
                    time_str,
                    symbol,
                    int(quantity),  # Convert to whole number
                    price,
                    side
                ])
                
            except (IndexError, ValueError) as e:
                print(f"Skipping invalid line: {line.strip()} - Error: {str(e)}")

if __name__ == '__main__':
    convert_tlg_to_csv('U15754950_20241218_20250203.tlg', 'trades1.csv')
    
