def build_backtest_context(backtest_summary: dict) -> str:
    return f"""
Backtest Summary:
- backtestRunId:{backtest_summary.get('backtestRunId')}
- datasetVersionId:{backtest_summary.get('datasetVersionId')}
- strategyId:{backtest_summary.get('strategyId')}
- totalReturn:{backtest_summary.get('totalReturn')}
- maxDrawdown:{backtest_summary.get('maxDrawdown')}
- sharpe:{backtest_summary.get('sharpe')}
- tradeCount:{backtest_summary.get('tradeCount')}
- winRate:{backtest_summary.get('winRate')}
""".strip()


def build_backtest_report_prompt(backtest_summary: dict) -> str:
    return f"""
You are a trading analysis assistant.

Please generate a concise backtest report.

{build_backtest_context(backtest_summary)}

Please provide:
1. Executive summary
2. Key strengths
3. Key risks
4. Suggestions for next iteration
""".strip()


def build_backtest_diagnosis_prompt(backtest_summary: dict) -> str:
    return f"""
You are a trading analysis assistant focused on diagnosing backtest weaknesses.

Please diagnose the backtest result with emphasis on:
1. Whether drawdown is too large
2. Whether trade frequency may be too high
3. Whether performance may be concentrated in a small number of favorable windows
4. Concrete next-step fixes

{build_backtest_context(backtest_summary)}
""".strip()
