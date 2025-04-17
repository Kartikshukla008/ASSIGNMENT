from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import io

app = FastAPI()


@app.post("/analyze-transactions/")
async def analyze_transactions(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        # Check for required columns
        required_cols = {"TransactionID", "UserID", "Date", "Amount", "Transaction Type"}
        if not required_cols.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"CSV file must contain columns: {required_cols}")

        # Clean data: drop rows with missing or invalid Amount/UserID/Type
        df = df.dropna(subset=["UserID", "Amount", "Transaction Type"])
        df = df[df["Transaction Type"].isin(["Credit", "Debit"])]
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
        df = df.dropna(subset=["Amount"])

        # Grouping for summary
        summary = df.groupby(["UserID", "Transaction Type"])["Amount"].sum().unstack(fill_value=0)
        summary = summary.reset_index()

        result_summary = summary.to_dict(orient="records")

        # Total transaction per user to get max spender
        df["Total"] = df["Amount"]
        max_user = df.groupby("UserID")["Total"].sum().idxmax()
        max_amount = df.groupby("UserID")["Total"].sum().max()

        return JSONResponse(content={
            "summary": result_summary,
            "highest_transaction_user": {
                "UserID": max_user,
                "TotalAmount": max_amount
            }
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
