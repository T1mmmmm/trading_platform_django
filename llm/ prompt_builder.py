def build_backtest_report_prompt(backtest_summary: dict) -> str:
    return f"""
You are a trading analysis assistant.

Please generate a concise backtest report.

Backtest Summary:
- totalReturn:{backtest_summary.get('totalReturn')}
- maxDrawdown:{backtest_summary.get('maxDrawdown')}
- sharpe:{backtest_summary.get('sharpe')}
- tradeCount:{backtest_summary.get('tradeCount')}
- winRate:{backtest_summary.get('winRate')}

Please provide:
1. Executive summary
2. Key strengths
3. Key risks
4. Suggestions for next iteration
""".strip()