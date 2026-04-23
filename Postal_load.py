import pandas as pd
import os

# New_output.xlsx가 있으면 사용, 없으면 output*.xlsx 파일들 사용
if os.path.exists("New_output.xlsx"):
    out_cleaned = pd.read_excel("New_output.xlsx", converters={'POSTAL': str})
else:
    out1 = pd.read_excel("output2.xlsx", converters={'POSTAL': str})
    out2 = pd.read_excel("output3.xlsx")
    out3 = pd.read_excel("output4.xlsx")
    out4 = pd.read_excel("output5.xlsx")
    out5 = pd.read_excel("output6.xlsx")
    out6 = pd.read_excel("output7.xlsx")
    out7 = pd.read_excel("output8.xlsx")
    out8 = pd.read_excel("output9.xlsx")
    out9 = pd.read_excel("output10.xlsx")
    out10 = pd.read_excel("output11.xlsx", converters={'POSTAL': str})
    
    out_cleaned = pd.concat([out1, out2, out3, out4, out5, out6, out7, out8, out9, out10], axis=0)

out_cleaned.to_excel('output_cleaned.xlsx', index=False)