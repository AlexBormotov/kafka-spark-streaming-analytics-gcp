-- Rebuffer ratio per CDN over the last 30 s: REBUFFER / (HEARTBEAT + REBUFFER) playback ticks,
-- as the generator defines it. One row per CDN becomes one alert instance labelled by cdn.
-- CDNs with fewer than 50 ticks are left out: too few to judge, and no rows means no alert.
SELECT
    cdn,
    sum(rebuffers) / (sum(heartbeats) + sum(rebuffers)) AS rebuffer_ratio
FROM qoe.qoe_10s
WHERE window_start >= now() - INTERVAL 30 SECOND
GROUP BY cdn
HAVING sum(heartbeats) + sum(rebuffers) >= 50
