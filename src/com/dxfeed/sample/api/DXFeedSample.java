/*
 * QDS - Quick Data Signalling Library
 * Copyright (C) 2002-2015 Devexperts LLC
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file, You can obtain one at
 * http://mozilla.org/MPL/2.0/.
 */
package com.dxfeed.sample.api;

import java.util.Arrays;
import java.util.List;

import com.dxfeed.api.*;
import com.dxfeed.event.market.*;

public class DXFeedSample {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 1) {
			System.err.println("usage: DXFeedSample <symbol>");
			System.err.println("where: <symbol>  is security symbol (e.g. IBM, C, SPX etc.)");
			return;
		}
		String symbol = args[0];
		testQuoteListener(symbol);
		testQuoteAndTradeListener(symbol);
		testTradeSnapshots(symbol);
	}

	private static void testQuoteListener(String symbol) {
		DXFeedSubscription<Quote> sub = DXFeed.getInstance().createSubscription(Quote.class);
		sub.addEventListener(new DXFeedEventListener<Quote>() {
			public void eventsReceived(List<Quote> quotes) {
				for (Quote quote : quotes)
					System.out.println("Mid = " + (quote.getBidPrice() + quote.getAskPrice()) / 2);
			}
		});
		sub.addSymbols(Arrays.asList(symbol));
	}

	@SuppressWarnings("unchecked")
	private static void testQuoteAndTradeListener(String symbol) {
		DXFeedSubscription<MarketEvent> sub = DXFeed.getInstance().<MarketEvent>createSubscription(Quote.class, Trade.class);
		sub.addEventListener(new DXFeedEventListener<MarketEvent>() {
			public void eventsReceived(List<MarketEvent> events) {
				for (MarketEvent event : events)
					System.out.println(event);
			}
		});
		sub.addSymbols(Arrays.asList(symbol));
	}

	private static void testTradeSnapshots(String symbol) throws InterruptedException {
		DXFeed feed = DXFeed.getInstance();
		DXFeedSubscription<Trade> sub = feed.createSubscription(Trade.class);
		sub.addSymbols(Arrays.asList(symbol));
		while (true) {
			System.out.println(feed.getLastEvent(new Trade(symbol)));
			Thread.sleep(1000);
		}
	}
}
