
import re

# Winning Numbers
W1 = "5828"
W2 = "4949"
W3 = "9274"

# User Input
USER_INPUT = """
02 x43 | 03 x53 | 04 x32 | 05 x55 | 06 x55 | 07 x12 | 09 x42 | 10 x22 | 11 x12 | 12 x197 | 13 x35 | 14 x26 | 15 x54 | 16 x112 | 17 x45 | 18 x132 | 19 x120 | 20 x22 | 21 x86 | 22 x43 | 26 x61 | 27 x51 | 29 x55 | 31 x22 | 33 x6 | 36 x30 | 50 x20 | 56 x12 | 58 x40 | 62 x30 | 63 x5 | 68 x3 | 70 x14 | 72 x24 | 73 x15 | 76 x8 | 77 x30 | 79 x44 | 81 x13 | 85 x26 | 86 x6 | 88 x9 | 89 x30 | 90 x20 | 91 x40 | 97 x11 | 8505 x1 | 6709 x1 | 8425 x1 | 1123 x1 | 4426 x1 | 1574 x1 | 4777 x1 | 1779 x1 | 6388 x1 | 2513 x1 | 5015 x1 | 6790 x1 | 0802 x2 | 1221 x1 | 4114 x1 | 0770 x2 | 5821 x2 | 6426 x1 | 1822 x1 | 6318 x1 | 6322 x1 | 4502 x1 | 4508 x1 | 4520 x1 | 4580 x1 | 4418 x1 | 2909 x2 | 9216 x1 | 7931 x1 | 6116 x1 | 6314 x1 | 7394 x1 | 1827 x1 | 9056 x1 | 2794 x2 | 1470 x1 | 5508 x1 | 7458 x1 | 7485 x1 | 8162 x1 | 3751 x1 | 5741 x1 | 1836 x1 | 3618 x1 | 1017 x1 | 1706 x1 | 2077 x3 | 3815 x3 | 8415 x1 | 5814 x1 | 7176 x1 | 1395 x1
"""

def calculate_single_ticket(num, bet, w1, w2, w3):
    num = str(num)
    w1 = str(w1) if w1 is not None else ""
    w2 = str(w2) if w2 is not None else ""
    w3 = str(w3) if w3 is not None else ""
    
    total_win = 0
    breakdown = []

    # LOGIC COPIED FROM lot_ticket_test.py (Simplified for Nacional only)
    
    if len(num) == 2:
        # Chances Logic
        if len(w1) >= 2 and num == w1[-2:]:
            win = bet * 14.00
            total_win += win
            breakdown.append(f"Chances (1er): $14.00 * {bet} = ${win:.2f}")
        if len(w2) >= 2 and num == w2[-2:]:
            win = bet * 3.00
            total_win += win
            breakdown.append(f"Chances (2do): $3.00 * {bet} = ${win:.2f}")
        if len(w3) >= 2 and num == w3[-2:]:
            win = bet * 2.00
            total_win += win
            breakdown.append(f"Chances (3er): $2.00 * {bet} = ${win:.2f}")

    elif len(num) == 4:
        # Billetes Logic
        prize_hits = []

        # Against 1st Prize
        if len(w1) == 4:
            if num == w1:
                prize_hits.append(("1er Premio (Exacto)", 2000.00))
            elif num[:3] == w1[:3]:
                prize_hits.append(("1er Premio (3 Primeras)", 50.00))
            elif num[-3:] == w1[-3:]:
                prize_hits.append(("1er Premio (3 Ultimas)", 50.00))
            elif num[:2] == w1[:2]:
                prize_hits.append(("1er Premio (2 Primeras)", 3.00))
            elif num[-2:] == w1[-2:]:
                prize_hits.append(("1er Premio (2 Ultimas)", 3.00))
            elif num[-1] == w1[-1]:
                prize_hits.append(("1er Premio (Ultima)", 1.00))

        # Against 2nd Prize
        if len(w2) == 4:
            if num == w2:
                prize_hits.append(("2do Premio (Exacto)", 600.00))
            elif num[:3] == w2[:3]:
                prize_hits.append(("2do Premio (3 Primeras)", 20.00))
            elif num[-3:] == w2[-3:]:
                prize_hits.append(("2do Premio (3 Ultimas)", 20.00))
            elif num[-2:] == w2[-2:]:
                prize_hits.append(("2do Premio (2 Ultimas)", 2.00))

        # Against 3rd Prize
        if len(w3) == 4:
            if num == w3:
                prize_hits.append(("3er Premio (Exacto)", 300.00))
            elif num[:3] == w3[:3]:
                prize_hits.append(("3er Premio (3 Primeras)", 10.00))
            elif num[-3:] == w3[-3:]:
                prize_hits.append(("3er Premio (3 Ultimas)", 10.00))
            elif num[-2:] == w3[-2:]:
                prize_hits.append(("3er Premio (2 Ultimas)", 1.00))

        for label, amount in prize_hits:
            win = bet * amount
            total_win += win
            breakdown.append(f"{label}: ${amount} * {bet} = ${win:.2f}")
            
    return total_win, breakdown

def main():
    # Parse input
    # Format: "02 x43 | 03 x53"
    items = []
    raw_items = [x.strip() for x in USER_INPUT.split('|')]
    
    for item in raw_items:
        if not item: continue
        parts = item.split('x')
        if len(parts) == 2:
            num = parts[0].strip()
            qty = int(parts[1].strip())
            items.append((num, qty))
        else:
            print(f"⚠️ Failed to parse: {item}")

    with open('result.txt', 'w', encoding='utf-8') as f:
        f.write(f"Parsed {len(items)} items.\\n")
        
        total_winnings = 0.0
        all_breakdowns = []
        
        for num, qty in items:
            win, lines = calculate_single_ticket(num, qty, W1, W2, W3)
            if win > 0:
                total_winnings += win
                all_breakdowns.append((num, qty, win, lines))

        f.write("\\n--- WINNING TICKETS ---\\n")
        for num, qty, win, lines in all_breakdowns:
            f.write(f"Ticket {num} (x{qty}) -> Won ${win:.2f}\\n")
            for line in lines:
                f.write(f"  - {line}\\n")
                
        f.write(f"\\nTOTAL WINNINGS: ${total_winnings:.2f}\\n")
        print("Done writing to result.txt")

if __name__ == "__main__":
    main()
