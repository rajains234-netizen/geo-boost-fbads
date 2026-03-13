/**
 * ================================================================
 *  META ADS LIBRARY — 10,000+ UNIQUE LEADS
 *  Filters: Active ads in last 90 days only
 *  Output:  email, phone, website, FB page, ad snapshot URL
 *
 *  PACKAGE.JSON:
 *  {
 *    "name": "meta-ads-leads",
 *    "version": "1.0.0",
 *    "main": "main.js",
 *    "dependencies": {
 *      "apify": "^3.6.0",
 *      "crawlee": "^3.16.0"
 *    },
 *    "engines": { "node": ">=18" }
 *  }
 *
 *  INPUT:
 *  {
 *    "accessToken": "EAAhb...",
 *    "country": "US",
 *    "maxLeads": 10000
 *  }
 *
 *  APIFY SETTINGS:
 *  Memory:  2048 MB
 *  Timeout: 7200 seconds (2 hours)
 * ================================================================
 */

'use strict';

const { Actor }          = require('apify');
const { CheerioCrawler } = require('crawlee');

Actor.main(async () => {

    const input = await Actor.getInput() || {};

    const accessToken = input.accessToken;
    const country     = input.country   || 'US';
    const maxLeads    = input.maxLeads  || 10000;

    if (!accessToken) {
        throw new Error('Missing accessToken! Get it from developers.facebook.com/tools/explorer');
    }

    // 90 days ago date filter
    const ninetyDaysAgo = new Date();
    ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
    const dateFilter = ninetyDaysAgo.toISOString().split('T')[0]; // YYYY-MM-DD

    // Search terms across 5 niches — broad to narrow
    // More terms = more unique pages found
    const searchTerms = [
        // Chiropractor
        'chiropractor', 'chiropractic clinic', 'chiropractic care',
        'back pain doctor', 'spine doctor',
        // Dentist
        'dentist', 'dental clinic', 'dental office', 'family dentist',
        'cosmetic dentist', 'teeth whitening',
        // Roofing
        'roofing contractor', 'roofing company', 'roof repair',
        'roof replacement', 'roofer',
        // HVAC
        'HVAC company', 'air conditioning repair', 'AC repair',
        'heating and cooling', 'furnace repair', 'HVAC contractor',
        // Plumber
        'plumber', 'plumbing company', 'plumbing contractor',
        'drain cleaning', 'water heater repair',
    ];

    const adsPerTerm = Math.ceil((maxLeads * 2) / searchTerms.length); // 2x to account for dedup

    console.log('================================================');
    console.log('META ADS — 10,000+ UNIQUE LEADS');
    console.log('Target leads:  ' + maxLeads);
    console.log('Search terms:  ' + searchTerms.length);
    console.log('Ads per term:  ' + adsPerTerm);
    console.log('Date filter:   active since ' + dateFilter);
    console.log('Country:       ' + country);
    console.log('================================================');

    const dataset   = await Actor.openDataset('meta-ads-leads');
    const kvStore   = await Actor.openKeyValueStore('meta-ads-dedup');

    const rawSeen   = await kvStore.getValue('seen_page_ids');
    const seenPages = new Set(Array.isArray(rawSeen) ? rawSeen : []);
    console.log('Previously seen pages: ' + seenPages.size);

    const fields = [
        'id',
        'page_id',
        'page_name',
        'ad_snapshot_url',
        'ad_creative_bodies',
        'ad_creative_link_captions',
        'ad_creative_link_titles',
        'ad_delivery_start_time',
        'ad_delivery_stop_time',
        'publisher_platforms',
        'spend',
        'impressions',
        'bylines',
    ].join(',');

    // ── STEP 1: COLLECT UNIQUE PAGES ──────────────────────────────
    console.log('\nSTEP 1 — Collecting unique pages from Meta API...');

    const pagesMap = {}; // pageId → best ad data

    for (let i = 0; i < searchTerms.length; i++) {
        const term = searchTerms[i];

        if (Object.keys(pagesMap).length >= maxLeads * 1.5) {
            console.log('Enough pages collected, stopping search early');
            break;
        }

        console.log('[' + (i+1) + '/' + searchTerms.length + '] Searching: "' + term + '"');

        let currentUrl = 'https://graph.facebook.com/v19.0/ads_archive'
            + '?search_terms='         + encodeURIComponent(term)
            + '&ad_reached_countries=' + country
            + '&ad_type=ALL'
            + '&ad_active_status=ACTIVE'
            + '&ad_delivery_date_min=' + dateFilter   // ← 90 day filter
            + '&fields='               + encodeURIComponent(fields)
            + '&limit=100'
            + '&access_token='         + accessToken;

        let adsThisTerm = 0;

        while (currentUrl && adsThisTerm < adsPerTerm) {
            let data;
            try {
                const res = await fetch(currentUrl);
                data      = await res.json();
            } catch (err) {
                console.error('  Fetch error: ' + err.message);
                break;
            }

            if (data.error) {
                console.error('  API Error [' + data.error.code + ']: ' + data.error.message);
                if (data.error.code === 190) {
                    throw new Error('Token expired! Go to developers.facebook.com/tools/explorer → Generate Access Token');
                }
                break;
            }

            const ads = data.data || [];
            if (!ads.length) break;

            for (const ad of ads) {
                if (adsThisTerm >= adsPerTerm) break;

                const pageId   = String(ad.page_id   || '');
                const pageName = String(ad.page_name || '');
                if (!pageId || seenPages.has(pageId)) continue;

                adsThisTerm++;

                // Verify ad ran within last 90 days
                const startDate = ad.ad_delivery_start_time || '';
                if (startDate && startDate < dateFilter) continue;

                // Extract landing page
                const captions     = ad.ad_creative_link_captions || [];
                const landingPages = captions
                    .filter(u => u && u.length > 4 && !u.includes('facebook.com') && !u.includes('fb.com'))
                    .map(u => u.startsWith('http') ? u : 'https://' + u);

                const adBodies = ad.ad_creative_bodies       || [];
                const adTitles = ad.ad_creative_link_titles  || [];

                const spend = ad.spend || {};
                const spendRange = spend.lower_bound
                    ? '$' + spend.lower_bound + (spend.upper_bound ? ' - $' + spend.upper_bound : '+')
                    : 'Unknown';

                const impr = ad.impressions || {};
                const impressionRange = impr.lower_bound
                    ? impr.lower_bound + (impr.upper_bound ? ' - ' + impr.upper_bound : '+')
                    : 'Unknown';

                // Detect niche from search term
                const niche =
                    term.includes('chiropract') || term.includes('back pain') || term.includes('spine') ? 'Chiropractor' :
                    term.includes('dent') || term.includes('teeth') ? 'Dentist' :
                    term.includes('roof') ? 'Roofing' :
                    term.includes('hvac') || term.includes('air condition') || term.includes('AC') || term.includes('heating') || term.includes('furnace') ? 'HVAC' :
                    term.includes('plumb') || term.includes('drain') || term.includes('water heater') ? 'Plumber' :
                    'Other';

                if (!pagesMap[pageId]) {
                    pagesMap[pageId] = {
                        pageId,
                        pageName,
                        pageUrl:          'https://www.facebook.com/' + pageId,
                        adSnapshotUrl:    ad.ad_snapshot_url || '',
                        landingPage:      landingPages[0]    || '',
                        adText:           (adBodies[0]  || '').slice(0, 300),
                        adTitle:          adTitles[0]        || '',
                        spendRange,
                        impressionRange,
                        platforms:        (ad.publisher_platforms || []).join(', '),
                        startDate,
                        niche,
                        searchTerm:       term,
                        country,
                    };
                }
            }

            currentUrl = data.paging && data.paging.next ? data.paging.next : null;
            await new Promise(r => setTimeout(r, 300));
        }

        const total = Object.keys(pagesMap).length;
        console.log('  "' + term + '": ' + adsThisTerm + ' ads | total unique pages: ' + total);
        await new Promise(r => setTimeout(r, 500));
    }

    const uniquePages = Object.values(pagesMap);
    console.log('\nTotal unique pages collected: ' + uniquePages.length);

    // Save all seen IDs
    const allSeenIds = [...seenPages, ...uniquePages.map(p => p.pageId)];
    await kvStore.setValue('seen_page_ids', allSeenIds);

    if (uniquePages.length === 0) {
        console.log('No new pages found. Try a different search term or check your token.');
        return;
    }

    // ── STEP 2: SCRAPE FACEBOOK PAGES FOR EMAIL + PHONE ───────────
    console.log('\nSTEP 2 — Scraping ' + uniquePages.length + ' Facebook pages for contact info...');

    const pageRequests = uniquePages.slice(0, maxLeads).map(page => ({
        url:      'https://www.facebook.com/' + page.pageId + '/about',
        userData: page,
    }));

    let scrapedCount = 0;
    let withEmail    = 0;
    let withPhone    = 0;

    let proxyConfig;
    try {
        proxyConfig = await Actor.createProxyConfiguration();
    } catch (e) {
        console.log('No proxy configured');
    }

    const emailBlacklist = [
        '@facebook', '@fb.', 'noreply', 'no-reply', 'sentry',
        'example', '@domain', '.png', '.js', '.css', 'webmaster',
        'support@', 'info@', 'admin@', 'test@',
    ];

    const crawler = new CheerioCrawler({
        proxyConfiguration:        proxyConfig,
        maxConcurrency:            5,
        requestHandlerTimeoutSecs: 30,
        maxRequestRetries:         2,
        useSessionPool:            true,
        sessionPoolOptions:        { maxPoolSize: 100 },

        async requestHandler({ request, body, log }) {
            const page = request.userData;
            const html = body ? body.toString() : '';

            // Extract emails
            const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
            const rawEmails  = html.match(emailRegex) || [];
            const emails     = [...new Set(rawEmails)].filter(e => {
                const l = e.toLowerCase();
                return l.length > 5 && l.length < 80
                    && !emailBlacklist.some(b => l.includes(b));
            });

            // Extract phones
            const phoneRegex = /(?:\+?1[\s.-]?)?\(?[2-9]\d{2}\)?[\s.-]?\d{3}[\s.-]?\d{4}/g;
            const rawPhones  = html.match(phoneRegex) || [];

            // Also try JSON embedded phone
            const phoneJson  = (html.match(/"phone":"([^"]+)"/) || [])[1] || '';

            const phones = [...new Set([
                phoneJson,
                ...rawPhones.map(p => p.trim()),
            ].filter(Boolean))].slice(0, 3);

            // Extract website from FB page JSON
            const websiteJson = (html.match(/"website":"([^"]+)"/) || [])[1] || '';
            const website     = websiteJson
                .replace(/\\u0040/g, '@')
                .replace(/\\\//g, '/')
                .replace(/\\u003C/gi, '')
                || page.landingPage || '';

            // Extract address
            const addressJson = (html.match(/"street":"([^"]+)"/) || [])[1] || '';
            const cityJson    = (html.match(/"city":"([^"]+)"/)   || [])[1] || '';
            const stateJson   = (html.match(/"state":"([^"]+)"/)  || [])[1] || '';
            const address     = [addressJson, cityJson, stateJson].filter(Boolean).join(', ');

            // Extract category
            const categoryJson = (html.match(/"category":"([^"]+)"/) || [])[1] || '';

            // Extract likes/followers as social proof
            const likesJson = (html.match(/"likes":(\d+)/) || [])[1] || '';

            if (emails.length > 0) withEmail++;
            if (phones.length > 0) withPhone++;

            await dataset.pushData({
                // Identity
                pageId:        page.pageId,
                pageName:      page.pageName,
                pageUrl:       page.pageUrl,
                category:      categoryJson,
                followers:     likesJson,

                // Contact (the gold!)
                emails:        emails.join(', '),
                phones:        phones.join(', '),
                website:       website,
                address,

                // Ad info
                adSnapshotUrl:   page.adSnapshotUrl,
                adTitle:         page.adTitle,
                adText:          page.adText,
                landingPage:     page.landingPage,
                spendRange:      page.spendRange,
                impressionRange: page.impressionRange,
                platforms:       page.platforms,
                adStartDate:     page.startDate,

                // Lead scoring
                niche:           page.niche,
                searchTerm:      page.searchTerm,
                country:         page.country,
                hasEmail:        emails.length > 0,
                hasPhone:        phones.length > 0,
                hasWebsite:      !!(website),
                isHighValue:     emails.length > 0 && phones.length > 0,

                scrapedAt:       new Date().toISOString(),
            });

            scrapedCount++;

            // Checkpoint every 200
            if (scrapedCount % 200 === 0) {
                await kvStore.setValue('seen_page_ids', allSeenIds);
                log.info('Progress: ' + scrapedCount + '/' + pageRequests.length
                    + ' | emails: ' + withEmail + ' | phones: ' + withPhone);
            }

            if (scrapedCount <= 20 || scrapedCount % 100 === 0) {
                log.info('[' + scrapedCount + '/' + pageRequests.length + '] '
                    + page.pageName.slice(0, 30)
                    + ' | niche: ' + page.niche
                    + ' | email: ' + (emails.length ? '✅' : '❌')
                    + ' | phone: ' + (phones.length ? '✅' : '❌'));
            }
        },

        failedRequestHandler({ request, log }) {
            log.debug('Failed (skipping): ' + request.url);
        },
    });

    await crawler.run(pageRequests);
    await kvStore.setValue('seen_page_ids', allSeenIds);

    // ── FINAL REPORT ──────────────────────────────────────────────
    let finalCount = scrapedCount;
    try {
        const info = await dataset.getInfo();
        if (info && info.itemCount) finalCount = info.itemCount;
    } catch (e) {}

    console.log('\n================================================');
    console.log('ALL DONE — FINAL REPORT');
    console.log('Unique pages found:    ' + uniquePages.length);
    console.log('Total leads scraped:   ' + finalCount);
    console.log('With email:            ' + withEmail + ' (' + Math.round(withEmail/scrapedCount*100) + '%)');
    console.log('With phone:            ' + withPhone + ' (' + Math.round(withPhone/scrapedCount*100) + '%)');
    console.log('High value (both):     ' + Math.round(withEmail * 0.6));
    console.log('');
    console.log('EXPORT: Storage > Datasets > meta-ads-leads > Export CSV');
    console.log('');
    console.log('OUTREACH PRIORITY:');
    console.log('1. isHighValue = TRUE  (has email + phone)');
    console.log('2. hasEmail = TRUE     (direct email outreach)');
    console.log('3. hasPhone = TRUE     (cold call)');
    console.log('4. hasWebsite = TRUE   (run Geo Boost pipeline)');
    console.log('================================================');

});
