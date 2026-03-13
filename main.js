'use strict';

const { Actor }          = require('apify');
const { CheerioCrawler } = require('crawlee');
const https              = require('https');

// Safe HTTP GET — works on all Node versions, no fetch needed
function httpGet(url) {
    return new Promise((resolve, reject) => {
        const req = https.get(url, (res) => {
            let data = '';
            res.on('data', chunk => { data += chunk; });
            res.on('end', () => {
                try { resolve(JSON.parse(data)); }
                catch (e) { reject(new Error('JSON parse error: ' + e.message + ' | data: ' + data.slice(0, 200))); }
            });
        });
        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
    });
}

Actor.main(async () => {

    console.log('Actor started successfully');

    const input = await Actor.getInput() || {};
    console.log('Input received: ' + JSON.stringify(Object.keys(input)));

    const accessToken = input.accessToken;
    const country     = input.country    || 'US';
    const maxLeads    = input.maxLeads   || 10000;

    if (!accessToken) {
        throw new Error('Missing accessToken in input! Go to developers.facebook.com/tools/explorer and generate one.');
    }

    // 90 days ago
    const ninetyDaysAgo = new Date();
    ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
    const dateFilter = ninetyDaysAgo.toISOString().split('T')[0];

    const searchTerms = [
        'chiropractor', 'chiropractic clinic', 'chiropractic care', 'back pain doctor', 'spine doctor',
        'dentist', 'dental clinic', 'dental office', 'family dentist', 'cosmetic dentist',
        'roofing contractor', 'roofing company', 'roof repair', 'roof replacement', 'roofer',
        'HVAC company', 'air conditioning repair', 'AC repair', 'heating and cooling', 'furnace repair',
        'plumber', 'plumbing company', 'plumbing contractor', 'drain cleaning', 'water heater repair',
    ];

    const adsPerTerm = Math.ceil((maxLeads * 2) / searchTerms.length);

    console.log('================================================');
    console.log('META ADS LIBRARY SCRAPER');
    console.log('Target leads:  ' + maxLeads);
    console.log('Search terms:  ' + searchTerms.length);
    console.log('Ads per term:  ' + adsPerTerm);
    console.log('Date filter:   active since ' + dateFilter);
    console.log('Country:       ' + country);
    console.log('================================================');

    const dataset = await Actor.openDataset('meta-ads-leads');
    const kvStore = await Actor.openKeyValueStore('meta-ads-dedup');

    const rawSeen   = await kvStore.getValue('seen_page_ids');
    const seenPages = new Set(Array.isArray(rawSeen) ? rawSeen : []);
    console.log('Previously seen pages: ' + seenPages.size);

    const fields = [
        'id', 'page_id', 'page_name', 'ad_snapshot_url',
        'ad_creative_bodies', 'ad_creative_link_captions',
        'ad_creative_link_titles', 'ad_delivery_start_time',
        'ad_delivery_stop_time', 'publisher_platforms',
        'spend', 'impressions',
    ].join(',');

    // ── STEP 1: COLLECT UNIQUE PAGES ──────────────────────────────
    console.log('\nSTEP 1 — Fetching ads from Meta API...');

    const pagesMap = {};

    for (let i = 0; i < searchTerms.length; i++) {
        const term = searchTerms[i];

        if (Object.keys(pagesMap).length >= maxLeads * 1.5) {
            console.log('Enough pages collected — stopping search early');
            break;
        }

        console.log('[' + (i+1) + '/' + searchTerms.length + '] Searching: "' + term + '"');

        let currentUrl = 'https://graph.facebook.com/v19.0/ads_archive'
            + '?search_terms='             + encodeURIComponent(term)
            + '&ad_reached_countries='     + country
            + '&ad_type=ALL'
            + '&ad_active_status=ACTIVE'
            + '&ad_delivery_date_min='     + dateFilter
            + '&fields='                   + encodeURIComponent(fields)
            + '&limit=100'
            + '&access_token='             + accessToken;

        let adsThisTerm = 0;

        while (currentUrl && adsThisTerm < adsPerTerm) {
            let data;
            try {
                data = await httpGet(currentUrl);
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

                const niche =
                    ['chiropractor','chiropractic','back pain','spine'].some(k => term.toLowerCase().includes(k)) ? 'Chiropractor' :
                    ['dentist','dental','teeth'].some(k => term.toLowerCase().includes(k))                        ? 'Dentist' :
                    ['roof','roofer'].some(k => term.toLowerCase().includes(k))                                   ? 'Roofing' :
                    ['hvac','air condition','ac repair','heating','furnace'].some(k => term.toLowerCase().includes(k)) ? 'HVAC' :
                    ['plumb','drain','water heater'].some(k => term.toLowerCase().includes(k))                    ? 'Plumber' :
                    'Other';

                if (!pagesMap[pageId]) {
                    pagesMap[pageId] = {
                        pageId,
                        pageName,
                        pageUrl:       'https://www.facebook.com/' + pageId,
                        adSnapshotUrl: ad.ad_snapshot_url || '',
                        landingPage:   landingPages[0]    || '',
                        adText:        (adBodies[0] || '').slice(0, 300),
                        adTitle:       adTitles[0]        || '',
                        spendRange,
                        impressionRange,
                        platforms:     (ad.publisher_platforms || []).join(', '),
                        startDate:     ad.ad_delivery_start_time || '',
                        niche,
                        searchTerm:    term,
                        country,
                    };
                }
            }

            currentUrl = data.paging && data.paging.next ? data.paging.next : null;
            await new Promise(r => setTimeout(r, 300));
        }

        const total = Object.keys(pagesMap).length;
        console.log('  Done: ' + adsThisTerm + ' ads | unique pages: ' + total);
        await new Promise(r => setTimeout(r, 500));
    }

    const uniquePages = Object.values(pagesMap);
    console.log('\nUnique pages collected: ' + uniquePages.length);

    const allSeenIds = [...seenPages, ...uniquePages.map(p => p.pageId)];
    await kvStore.setValue('seen_page_ids', allSeenIds);

    if (uniquePages.length === 0) {
        console.log('No pages found. Check your token and try again.');
        return;
    }

    // ── STEP 2: SCRAPE FACEBOOK PAGES ─────────────────────────────
    console.log('\nSTEP 2 — Scraping ' + uniquePages.length + ' Facebook pages for email + phone...');

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
        console.log('No proxy configured — continuing without');
    }

    const emailBlacklist = [
        '@facebook', '@fb.', 'noreply', 'no-reply', 'sentry',
        'example', '@domain', '.png', '.js', '.css',
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

            const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
            const emails = [...new Set(html.match(emailRegex) || [])].filter(e => {
                const l = e.toLowerCase();
                return l.length > 5 && l.length < 80 && !emailBlacklist.some(b => l.includes(b));
            });

            const phoneRegex = /(?:\+?1[\s.-]?)?\(?[2-9]\d{2}\)?[\s.-]?\d{3}[\s.-]?\d{4}/g;
            const phoneJson  = (html.match(/"phone":"([^"]+)"/) || [])[1] || '';
            const phones     = [...new Set([phoneJson, ...(html.match(phoneRegex) || []).map(p => p.trim())].filter(Boolean))].slice(0, 3);

            const websiteJson = (html.match(/"website":"([^"]+)"/) || [])[1] || '';
            const website     = websiteJson.replace(/\\u0040/g, '@').replace(/\\\//g, '/') || page.landingPage || '';
            const address     = [(html.match(/"street":"([^"]+)"/) || [])[1], (html.match(/"city":"([^"]+)"/) || [])[1], (html.match(/"state":"([^"]+)"/) || [])[1]].filter(Boolean).join(', ');
            const category    = (html.match(/"category":"([^"]+)"/) || [])[1] || '';
            const followers   = (html.match(/"likes":(\d+)/)        || [])[1] || '';

            if (emails.length > 0) withEmail++;
            if (phones.length > 0) withPhone++;

            await dataset.pushData({
                pageId:          page.pageId,
                pageName:        page.pageName,
                pageUrl:         page.pageUrl,
                category,
                followers,
                emails:          emails.join(', '),
                phones:          phones.join(', '),
                website,
                address,
                adSnapshotUrl:   page.adSnapshotUrl,
                adTitle:         page.adTitle,
                adText:          page.adText,
                landingPage:     page.landingPage,
                spendRange:      page.spendRange,
                impressionRange: page.impressionRange,
                platforms:       page.platforms,
                adStartDate:     page.startDate,
                niche:           page.niche,
                searchTerm:      page.searchTerm,
                country:         page.country,
                hasEmail:        emails.length > 0,
                hasPhone:        phones.length > 0,
                hasWebsite:      !!website,
                isHighValue:     emails.length > 0 && phones.length > 0,
                scrapedAt:       new Date().toISOString(),
            });

            scrapedCount++;

            if (scrapedCount % 200 === 0) {
                await kvStore.setValue('seen_page_ids', allSeenIds);
                log.info('Progress: ' + scrapedCount + '/' + pageRequests.length + ' | emails: ' + withEmail + ' | phones: ' + withPhone);
            }

            if (scrapedCount <= 10 || scrapedCount % 100 === 0) {
                log.info('[' + scrapedCount + '/' + pageRequests.length + '] ' + page.pageName.slice(0, 30) + ' | ' + page.niche + ' | email:' + (emails.length ? '✅' : '❌') + ' phone:' + (phones.length ? '✅' : '❌'));
            }
        },

        failedRequestHandler({ request, log }) {
            log.debug('Failed (skipping): ' + request.url);
        },
    });

    await crawler.run(pageRequests);
    await kvStore.setValue('seen_page_ids', allSeenIds);

    let finalCount = scrapedCount;
    try {
        const info = await dataset.getInfo();
        if (info && info.itemCount) finalCount = info.itemCount;
    } catch (e) {}

    console.log('\n================================================');
    console.log('ALL DONE');
    console.log('Total leads:   ' + finalCount);
    console.log('With email:    ' + withEmail + ' (' + (scrapedCount ? Math.round(withEmail/scrapedCount*100) : 0) + '%)');
    console.log('With phone:    ' + withPhone + ' (' + (scrapedCount ? Math.round(withPhone/scrapedCount*100) : 0) + '%)');
    console.log('High value:    ' + Math.round(Math.min(withEmail, withPhone) * 0.7));
    console.log('EXPORT: Storage > Datasets > meta-ads-leads > Export CSV');
    console.log('================================================');

});
