import pandas as pd
import numpy as np

def process_sheet(file_path, sheet_name):
    try:
        # Read first few rows to find header
        # We look for a row containing 'Strike' or 'Chg in OI Value'
        df_preview = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=10)
        
        header_row_idx = -1
        for i, row in df_preview.iterrows():
            # Convert row to string and check for keywords
            row_str = row.astype(str).str.lower().tolist()
            if 'strike' in row_str or 'chg in oi value' in row_str:
                header_row_idx = i
                break
        
        if header_row_idx == -1:
            print(f"Could not find header row in sheet {sheet_name}")
            return pd.DataFrame()
            
        # Read identifying the header row
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row_idx)
        
        # Convert columns to numeric, coercing errors to NaN
        # Also trim column names to handle potential whitespace
        df.columns = df.columns.astype(str).str.strip()
        
        df['Strike'] = pd.to_numeric(df['Strike'], errors='coerce')
        df['Call_Chg_OI_Val'] = pd.to_numeric(df['Chg in OI Value'], errors='coerce')
        df['Call_VWAP'] = pd.to_numeric(df['VWAP'], errors='coerce')
        df['Call_LTP'] = pd.to_numeric(df['LTP (Chg %)'], errors='coerce')
        
        df['Put_Chg_OI_Val'] = pd.to_numeric(df['Chg in OI Value.1'], errors='coerce')
        df['Put_VWAP'] = pd.to_numeric(df['VWAP.1'], errors='coerce')
        df['Put_LTP'] = pd.to_numeric(df['LTP (Chg %).1'], errors='coerce')

        # Drop rows where Strike is NaN
        df = df.dropna(subset=['Strike'])
        
        # Fill NaNs with 0 for aggregation of Chg OI
        cols_to_fill = ['Call_Chg_OI_Val', 'Put_Chg_OI_Val', 'Call_VWAP', 'Put_VWAP', 'Call_LTP', 'Put_LTP']
        df[cols_to_fill] = df[cols_to_fill].fillna(0)
        
        # Return RAW columns so we can decide later based on consistency
        return df[['Strike', 'Call_Chg_OI_Val', 'Call_VWAP', 'Call_LTP', 'Put_Chg_OI_Val', 'Put_VWAP', 'Put_LTP']]
    except Exception as e:
        print(f"Error processing sheet {sheet_name}: {e}")
        # Debug: Print columns to help identify why 'Strike' might be missing
        try:
             print(f"Columns in {sheet_name}: {df.columns.tolist()}")
        except:
             pass
        return pd.DataFrame()

def calculate_levels(file_path, sheet_names):
    all_data = []
    
    # Process each sheet
    for sheet in sheet_names:
        print(f"Processing sheet: {sheet}")
        df = process_sheet(file_path, sheet)
        if not df.empty:
            df['Sheet_Index'] = sheet_names.index(sheet) # Track order
            all_data.append(df)
    
    if not all_data:
        print("No valid data found.")
        return

    # Aggregate data
    combined_df = pd.concat(all_data)
    
    # Logic for Consistency:
    # If the FIRST day (sheet_index 0) uses LTP (because VWAP was 0), then ALL days must use LTP.
    # Otherwise, they use their own calculated Ref Price (VWAP unless 0).
    # Wait, the user said: "change the logic to use ltp for all days if we use ltp for the first day"
    # This implies we need to check the Source for the first day.
    
    # Let's adjust the dataframe to include columns for pure VWAP and LTP again so we can recalculate ref price during aggregation if needed.
    # Actually, process_sheet calculates Ref_Price locally. 
    # We should probably change process_sheet to return Source info or just raw values.
    # Reworking process_sheet is cleaner.
    
    # RE-DEFINING LOGIC:
    # 1. Gather Call_VWAP, Call_LTP, Put_VWAP, Put_LTP for each day.
    # 2. Group by Strike.
    # 3. Check Day 1 (first sheet) VWAP. 
    #    If Day 1 VWAP <= 0:
    #       Call_Ref_Price_Daily = Call_LTP (for all days)
    #    Else:
    #       Call_Ref_Price_Daily = Call_VWAP (if > 0 else Call_LTP) (per day standard logic)
    
    # To do this efficiently with pandas:
    # We need to preserve the daily rows.
    
    def calculate_group_ref_price(group, side='Call'):
        # Sort by Sheet Index to ensure chronological order
        group = group.sort_values('Sheet_Index')
        
        first_day = group.iloc[0]
        vwap_col = f'{side}_VWAP'
        ltp_col = f'{side}_LTP'
        
        # Check First Day Condition
        # If first day VWAP is valid (>0), we stick to standard logic (VWAP, fallback to LTP per day)
        # If first day VWAP is INVALID (<=0), we force LTP for ALL days.
        
        force_ltp = False
        if first_day[vwap_col] <= 0:
            force_ltp = True
            
        prices = []
        for _, row in group.iterrows():
            if force_ltp:
                price = row[ltp_col]
            else:
                # Standard logic: VWAP if valid, else LTP
                price = row[vwap_col] if row[vwap_col] > 0 else row[ltp_col]
            prices.append(price)
            
        return pd.Series({'Avg_Ref_Price': np.mean(prices)})

    grouped = combined_df.groupby('Strike')
    
    # 1. Sum Change in OI
    agg_oi = grouped[['Call_Chg_OI_Val', 'Put_Chg_OI_Val']].sum()
    
    # 2. Calculate Average Ref Price with Consistency Logic
    # This apply is a bit slow but safe for logic complexity
    call_prices = grouped.apply(calculate_group_ref_price, side='Call', include_groups=False)
    put_prices = grouped.apply(calculate_group_ref_price, side='Put', include_groups=False)
    
    # Merge results
    dataset = agg_oi.join(call_prices.rename(columns={'Avg_Ref_Price': 'Call_Ref_Price'}))
    dataset = dataset.join(put_prices.rename(columns={'Avg_Ref_Price': 'Put_Ref_Price'}))
    dataset = dataset.reset_index()

    # --- Call Side (Resistance) ---
    top_calls = dataset.sort_values(by='Call_Chg_OI_Val', ascending=False).head(5)
    
    print("\n--- TOP 5 RESISTANCE LEVELS (Calls) ---")
    print(f"{'Rank':<5} {'Strike':<10} {'Cum Chg OI':<15} {'Avg Ref Price':<15} {'Resistance':<15}")
    print("-" * 75)
    
    rank = 1
    for _, row in top_calls.iterrows():
        resistance = row['Strike'] + row['Call_Ref_Price']
        print(f"{rank:<5} {row['Strike']:<10} {row['Call_Chg_OI_Val']:<15.2f} {row['Call_Ref_Price']:<15.2f} {resistance:<15.2f}")
        rank += 1
        
    # --- Put Side (Support) ---
    top_puts = dataset.sort_values(by='Put_Chg_OI_Val', ascending=False).head(5)
    
    print("\n--- TOP 5 SUPPORT LEVELS (Puts) ---")
    print(f"{'Rank':<5} {'Strike':<10} {'Cum Chg OI':<15} {'Avg Ref Price':<15} {'Support':<15}")
    print("-" * 75)
    
    rank = 1
    for _, row in top_puts.iterrows():
        support = row['Strike'] - row['Put_Ref_Price']
        print(f"{rank:<5} {row['Strike']:<10} {row['Put_Chg_OI_Val']:<15.2f} {row['Put_Ref_Price']:<15.2f} {support:<15.2f}")
        rank += 1

if __name__ == "__main__":
    # Configuration
    target_file = 'Nifty 10th Feb expiry.xlsx'
    target_sheets = ['tue6', 'wed6', 'THU6', 'fri6']
    
    # Fallback configuration (for testing/dev environment where specific file might be missing)
    fallback_file = 'tuesday file.xlsx'
    
    import os
    
    if os.path.exists(target_file):
        print(f"Found target file: {target_file}")
        # Check if sheets exist? process_sheet handles errors for missing sheets, but let's see.
        calculate_levels(target_file, target_sheets)
    elif os.path.exists(fallback_file):
        print(f"Target file '{target_file}' not found.")
        print(f"Found fallback file: {fallback_file}")
        print("Processing all sheets in fallback file for demonstration...")
        try:
            xl = pd.ExcelFile(fallback_file)
            print(f"Sheets found: {xl.sheet_names}")
            calculate_levels(fallback_file, xl.sheet_names)
        except Exception as e:
            print(f"Error reading fallback file: {e}")
    else:
        print(f"Neither '{target_file}' nor '{fallback_file}' found.")
