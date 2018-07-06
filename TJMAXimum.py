from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.factors import RSI
import talib
import numpy as np
import pandas as pd
from quantopian.pipeline import CustomFactor
from quantopian.pipeline.data import morningstar

hpdict = {}

class Liquidity(CustomFactor):   
    
    inputs = [USEquityPricing.volume, morningstar.valuation.shares_outstanding]
    window_length = 1
    
    def compute(self, today, assets, out, volume, shares):       
        out[:] = volume[-1]/shares[-1]
        
class Quality(CustomFactor):   
    
    inputs = [morningstar.operation_ratios.roe] 
    window_length = 1
    
    def compute(self, today, assets, out, roe):       
        out[:] = roe[-1]
        
# Create custom factor to calculate a market cap based on yesterday's close
# We'll use this to get the top 2000 stocks by market cap
class MarketCap(CustomFactor):   
    
    inputs = [USEquityPricing.close, morningstar.valuation.shares_outstanding] 
    window_length = 1
    
    def compute(self, today, assets, out, close, shares):       
        out[:] = close[-1] * shares[-1]
        

def initialize(context):
    attach_pipeline(make_pipeline(context),'ranked_500')
    #set_benchmark(sid(16841))
    #context.aapl=sid(16841)
 
    context.max_notional=1000000
    context.min_notional=-1000000
   
    total_minutes = 6*60 + 30
    schedule_function(func = closehp,date_rule=date_rules.every_day(),time_rule=time_rules.market_open(minutes=60))
    
    for i in range(1, total_minutes):
        if i % 5 == 0:
            schedule_function(func = tradevwap2,date_rule=date_rules.every_day(),time_rule=time_rules.market_open(minutes=i))
    # This will start at 9:31AM and will run every 30 minutes
            #schedule_function(tradevwap,date_rules.every_day(),time_rules.market_open(minutes = i))

    #for i in range(1, total_minutes):
    #    if i % 5 == 0:
    #        schedule_function(func = close_trade,date_rule=date_rules.every_day(),time_rule=time_rules.market_open(minutes=i))
    
    context.long_leverage = 0.50
    context.short_leverage = -0.50
    
def make_pipeline(context):
    pipe = Pipeline()
    
    # Add the two factors defined to the pipeline
    liquidity = Liquidity()
    pipe.add(liquidity, 'liquidity')
    
    quality = Quality()
    pipe.add(quality, 'roe')
    
    mkt_cap = MarketCap()
    pipe.add(mkt_cap, 'market cap')
    top_2000 = mkt_cap.top(1000)
    #1000 for best backtest result
    
    liquidity_rank = liquidity.rank(mask=top_2000)
    pipe.add(liquidity_rank, 'liq_rank')
    top_500 = liquidity_rank.top(200)
    #200 for best backtest result
    
    quality_rank = quality.rank(mask=top_500)
    pipe.add(quality_rank, 'roe_rank')
    
    # Set a screen to ensure that only the top 2000 companies by market cap 
    # with a roe > ___ are returned
    pipe.set_screen(top_500)
    return pipe

def before_trading_start(context, data):
    holdingperiod(context,data)
    # Call pipeline_output to get the output
    context.output = pipeline_output('ranked_500')
      
    # Narrow down the securities to only the top 200 & update my universe
    context.long_list = context.output.sort(['roe_rank'], ascending=False).iloc[:400]
    context.short_list = context.output.sort(['roe_rank'], ascending=False).iloc[-400:]
    context.stocksToTrade = context.output.sort(['liq_rank'],ascending=False)
    
   

def handle_data(context, data):
    close_trade(context,data)


def hurst(data, sid):
    #gather_prices(context, data, sid, 40)
    #data_gathered = gather_data(data)
    hist = data.history(sid, 'price', 1000, '1m')
    if hist is None:
        return    

    tau, lagvec = [], []
    for lag in range(2,100):  
        pp = np.subtract(hist[lag:],hist[:-lag])
        lagvec.append(lag)
        tau.append(np.sqrt(np.std(pp)))
    m = np.polyfit(np.log10(lagvec),np.log10(tau),1)
    hurst = m[0]*2
    return hurst

def rsi(context, data, stock):
    window = 9
    hist = data.history(stock, 'price', 12, '1d')
        # Calculate RSI moving average
    rsi = talib.RSI(hist, timeperiod=window)
    context.rsi_current = rsi[-1] # Get most recent RSI of the stock
    context.rsi_mean = np.nanmean(rsi) 
    return context.rsi_current

def vwap(context, data, stock):
    vwap_hist = data.history(stock, ['price', 'volume'], 15, '1d')[:-1]
    #15 initially
    mvwap = (vwap_hist['price'] * vwap_hist['volume']).sum() / vwap_hist['volume'].sum()
    return mvwap
    # Implement your algorithm logic here.
def calchurst(context,data):
    for stock in context.stocksToTrade.index:
        h = hurst(data,stock)
        if h > 0.5:
            print h, stock
        
def tradevwap2(context,data):
    portfolio_val = sum([abs(context.portfolio.positions[stock].amount*data.current(stock,'price')) for stock in context.stocksToTrade.index])
    print "portfolio_val", portfolio_val
    if portfolio_val < 2500000:
        for stock in context.stocksToTrade.index:
            h = hurst(data, stock)
            prev_price = data.history(stock, 'price', 120, '1m')[0]
            mvwap = vwap(context, data,stock)
            price=data.current(stock,'price')
            under_lim = abs(context.portfolio.positions[stock].amount * price) < 1000000
            if h < 0.5:
                return
            elif mvwap is None:
                return        
            elif price > mvwap  and prev_price < mvwap:
                print h, stock,"ordering longs"
            #log.info("ordering longs")
            #log.info("stock name: ", stock)
                if under_lim and not get_open_orders(stock):
                    order_target_value(stock, 500000)
            elif  price < mvwap * 0.91 and prev_price > mvwap * 0.91:
                print h, stock, "ordering shorts"
            #log.info("ordering shorts")
            #log.info("stock name: ", stock)
                if under_lim and not get_open_orders(stock):
                    order_target_value(stock, -500000)
            
def tradevwap(context,data):
    #if len(context.long_list) == 0:
    #    return
    #else:
    #    long_weight = context.long_leverage / float(len(context.long_list))
    #if len(context.short_list) == 0:
    #    return
    #else:
    #    short_weight = context.short_leverage / float(len(context.short_list))
    for long_stock in context.long_list.index:
        h = hurst(data, long_stock)
        prev_price = data.history(long_stock, 'price', 120, '1m')[0]
        #hurs = hurst(context, data, long_stock)
        mvwap = vwap(context, data,long_stock)
        price=data.current(long_stock,'price')
        if h < 0.5:
            return
        elif mvwap is None:
            return        
        elif price > mvwap  and prev_price < mvwap:
            log.info("ordering longs")
            log.info("stock name: ", long_stock)
            order_target_percent(long_stock, .5)
        
            
    for short_stock in context.short_list.index:
        h = hurst(data, short_stock)
        prev_price = data.history(short_stock, 'price', 120, '1m')[0]
        mvwap = vwap(context, data, short_stock)
        price=data.current(short_stock,'price')
        
        if h < 0.5:
            return
        elif mvwap is None:
            return      
        elif  price < mvwap * 0.91 and prev_price > mvwap * 0.91:
            log.info("ordering shorts")
            log.info("stock name: ", short_stock)
            order_target_percent(short_stock, -.5)

def holdingperiod(context,data):
    for stock in context.portfolio.positions.iterkeys():
        if stock in hpdict.keys():
            if hpdict[stock] > 14:
                hpdict[stock] = 0
            else:
                hpdict[stock] += 1
        else:
            hpdict[stock] = 1

def closehp(context,data):
    for stock in context.portfolio.positions.iterkeys():
        if stock not in hpdict.keys():
            return
        elif hpdict[stock] > 10 and not get_open_orders(stock):
            order_target_percent(stock,0)
            "Close trade 10 days"
        
def close_trade(context,data):
    for stock in context.portfolio.positions.iterkeys():
        price=data.current(stock,'price')
        trade_price = context.portfolio.positions[stock].cost_basis
        long = context.portfolio.positions[stock].amount > 10
        short = context.portfolio.positions[stock].amount < -10
        #if stock not in context.long_list.index and stock not in context.short_list.index:
        #    order_target(stock, 0)
        #closing short position
        if long and not get_open_orders(stock):
            if (price-trade_price)/trade_price < -.03:
                print "stop loss on long pos", stock
                order_target(stock,0)
            elif (price-trade_price)/trade_price > .10:
                print "take profit on lon pos"
                order_target(stock,0)
        elif short and not get_open_orders(stock):
            if -(price-trade_price)/trade_price < -.03:
                print "stop loss on short pos", stock
                order_target(stock,0)
            elif -(price-trade_price)/trade_price > .10:
                print "take profit on short pos"
                order_target(stock,0)
        
