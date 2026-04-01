package eu.kanade.tachiyomi.data.track.mangabaka

import android.net.Uri
import androidx.core.net.toUri
import eu.kanade.tachiyomi.BuildConfig
import eu.kanade.tachiyomi.data.database.models.Track
import eu.kanade.tachiyomi.data.track.TrackerManager
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaItem
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaItemResult
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaListEntry
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaListResult
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaOAuth
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaSearchResult
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaUserProfileResponse
import eu.kanade.tachiyomi.data.track.model.TrackMangaMetadata
import eu.kanade.tachiyomi.data.track.model.TrackSearch
import eu.kanade.tachiyomi.network.DELETE
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.HttpException
import eu.kanade.tachiyomi.network.POST
import eu.kanade.tachiyomi.network.PUT
import eu.kanade.tachiyomi.network.awaitSuccess
import eu.kanade.tachiyomi.network.parseAs
import eu.kanade.tachiyomi.util.PkceUtil
import eu.kanade.tachiyomi.util.lang.htmlDecode
import eu.kanade.tachiyomi.util.lang.toLocalDate
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.FormBody
import okhttp3.Headers.Companion.headersOf
import okhttp3.OkHttpClient
import okhttp3.RequestBody.Companion.toRequestBody
import tachiyomi.core.common.util.lang.withIOContext
import uy.kohesive.injekt.injectLazy
import java.math.RoundingMode
import java.util.Collections
import java.util.Locale
import kotlin.time.Instant
import tachiyomi.domain.track.model.Track as DomainTrack

class MangaBakaApi(
    private val trackId: Long,
    baseClient: OkHttpClient,
    interceptor: MangaBakaInterceptor,
) {

    private val json: Json by injectLazy()

    private val client = baseClient.newBuilder().addInterceptor {
        it.request().newBuilder()
            .header("User-Agent", "Komikku/v${BuildConfig.VERSION_NAME} (Android) (https://github.com/xkana-shii/komikku)")
            .build()
            .let(it::proceed)
    }.build()

    private val authClient = client.newBuilder().addInterceptor(interceptor).build()

    suspend fun addLibManga(
        track: Track,
        knownSeriesData: MangaBakaItem? = null,
        numberOfRereads: Int = 0,
    ): Track {
        return withIOContext {
            val seriesData = knownSeriesData ?: fetchSeriesData(track.remote_id)
            val resolvedId = seriesData.mergedWith ?: seriesData.id
            track.remote_id = resolvedId
            val url = "$LIBRARY_API_URL/$resolvedId"
            val body = buildJsonObject {
                put("is_private", track.private)
                put("state", track.toApiStatus())
                if (track.last_chapter_read > 0.0) {
                    put("progress_chapter", track.last_chapter_read)
                }
                if (track.score > 0) {
                    put("rating", track.score.toInt().coerceIn(0, 100))
                }
                if (track.started_reading_date > 0) {
                    put("start_date", track.started_reading_date.toLocalDate().toString())
                }
                if (track.finished_reading_date > 0) {
                    put("finish_date", track.finished_reading_date.toLocalDate().toString())
                }
                if (numberOfRereads > 0) {
                    put("number_of_rereads", numberOfRereads)
                }
            }
                .toString()
                .toRequestBody()

            authClient
                .newCall(POST(url, body = body, headers = headersOf("Content-Type", APP_JSON)))
                .awaitSuccess()
            libraryCache.remove(resolvedId)
            seriesCache.remove(resolvedId)

            // only returns 201 with the body { "status": 201, "data": true }, so no library ID for us
            track.title = seriesData.title
            track.total_chapters = seriesData.totalChapters?.toLongOrNull() ?: 0
            track
        }
    }

    suspend fun deleteLibManga(track: DomainTrack) {
        withIOContext {
            val resolvedId = resolveId(track.remoteId)
            val url = "$LIBRARY_API_URL/$resolvedId"

            authClient
                .newCall(DELETE(url))
                .awaitSuccess()
            libraryCache.remove(resolvedId)
            seriesCache.remove(resolvedId)
        }
    }

    suspend fun findLibManga(track: Track): Track? {
        return withIOContext {
            try {
                val originalId = track.remote_id
                android.util.Log.d("MangaBakaTest", "findLibManga: originalId=$originalId")

                val (userData, seriesResult) = coroutineScope {
                    val libraryDeferred = async {
                        fetchLibraryEntry(originalId)
                    }

                    val seriesDeferred = async {
                        runCatching { fetchSeriesData(originalId) }
                    }

                    libraryDeferred.await() to seriesDeferred.await()
                }

                android.util.Log.d("MangaBakaTest", "findLibManga: userData for $originalId = $userData")

                val additionalData = seriesResult.getOrElse { e ->
                    if (e is HttpException && e.code == 404) {
                        android.util.Log.d("MangaBakaTest", "findLibManga: series 404 for originalId=$originalId, resolving id")
                        fetchSeriesData(resolveId(originalId))
                    } else {
                        throw e
                    }
                }

                val resolvedId = additionalData.mergedWith ?: additionalData.id
                android.util.Log.d("MangaBakaTest", "findLibManga: resolvedId=$resolvedId")

                if (userData == null) {
                    android.util.Log.d("MangaBakaTest", "findLibManga: no library entry for originalId=$originalId (no auto-create)")
                    return@withIOContext null
                }

                // MERGE-ONLY AUTO-CREATE
                val resolvedEntry = if (resolvedId != originalId) {
                    val entry = fetchLibraryEntry(resolvedId)
                    android.util.Log.d("MangaBakaTest", "findLibManga: resolvedEntry for $resolvedId = $entry")

                    // Always merge best data from both entries, regardless of whether
                    // the resolved ID already has an entry or not.
                    val mergedEntry = mergeBestEntry(userData, entry)
                    android.util.Log.d("MangaBakaTest", "findLibManga: mergedEntry for $resolvedId = $mergedEntry")

                    // Build a track from the merged best data — used for both create and update paths
                    val mergedTrack = Track.create(TrackerManager.MANGABAKA).apply {
                        remote_id = resolvedId
                        title = additionalData.title
                        status = mergedEntry.getStatus()
                        score = mergedEntry.rating?.toDouble() ?: 0.0
                        started_reading_date = mergedEntry.startDate?.let { Instant.parse(it).toEpochMilliseconds() } ?: 0
                        finished_reading_date = mergedEntry.finishDate?.let { Instant.parse(it).toEpochMilliseconds() } ?: 0
                        last_chapter_read = mergedEntry.progressChapter ?: 0.0
                        total_chapters = additionalData.totalChapters?.toLongOrNull() ?: 0
                        private = mergedEntry.isPrivate
                    }

                    if (entry == null) {
                        android.util.Log.d(
                            "MangaBakaTest",
                            "findLibManga: merged series detected, creating entry for resolvedId=$resolvedId from originalId=$originalId",
                        )
                        addLibManga(
                            mergedTrack,
                            knownSeriesData = additionalData,
                            numberOfRereads = mergedEntry.numberOfRereads ?: 0,
                        )
                    } else {
                        // Resolved entry existed but needs to be updated with the merged best data
                        android.util.Log.d(
                            "MangaBakaTest",
                            "findLibManga: updating existing resolvedId=$resolvedId with merged best data",
                        )
                        updateLibManga(
                            mergedTrack,
                            knownSeriesData = additionalData,
                            knownEntry = mergedEntry,
                        )
                    }
                    // Either way, return mergedEntry so the final Track.create uses best data
                    mergedEntry
                } else {
                    null
                }

                // If we have a resolvedEntry (merged best data), use it.
                // Only fall back to userData when this is not a merged series.
                val finalEntry = resolvedEntry ?: userData

                val totalChaptersLong = additionalData.totalChapters?.toLongOrNull()

                Track.create(TrackerManager.MANGABAKA).apply {
                    remote_id = resolvedId
                    title = additionalData.title
                    status = finalEntry.getStatus()
                    score = finalEntry.rating?.toDouble() ?: 0.0
                    started_reading_date = finalEntry.startDate?.let { Instant.parse(it).toEpochMilliseconds() } ?: 0
                    finished_reading_date =
                        finalEntry.finishDate?.let { Instant.parse(it).toEpochMilliseconds() } ?: 0
                    last_chapter_read = finalEntry.progressChapter ?: 0.0
                    total_chapters = totalChaptersLong ?: 0
                    private = finalEntry.isPrivate
                }.also {
                    android.util.Log.d(
                        "MangaBakaTest",
                        "findLibManga: returning Track(remote_id=${it.remote_id}, last_chapter_read=${it.last_chapter_read}, score=${it.score}, private=${it.private}, started=${it.started_reading_date}, finished=${it.finished_reading_date})",
                    )
                }
            } catch (e: HttpException) {
                if (e.code == 404) {
                    android.util.Log.d("MangaBakaTest", "findLibManga: HttpException 404 for originalId=${track.remote_id}")
                    return@withIOContext null
                } else {
                    throw e
                }
            }
        }
    }

    suspend fun updateLibManga(track: Track, knownSeriesData: MangaBakaItem? = null, knownEntry: MangaBakaListEntry? = null): Track {
        return withIOContext {
            val originalId = track.remote_id
            android.util.Log.d("MangaBakaTest", "updateLibManga: originalId=$originalId")

            val (seriesData, entry) = coroutineScope {
                val seriesDeferred = if (knownSeriesData != null) {
                    null
                } else {
                    async { fetchSeriesData(originalId) }
                }

                val entryDeferred = if (knownEntry != null) {
                    null
                } else {
                    async {
                        runCatching {
                            fetchLibraryEntry(originalId)
                        }.getOrNull()
                    }
                }

                (seriesDeferred?.await() ?: knownSeriesData!!) to
                    (entryDeferred?.await() ?: knownEntry)
            }

            android.util.Log.d("MangaBakaTest", "updateLibManga: entry for $originalId = $entry")

            val resolvedId = seriesData.mergedWith ?: seriesData.id
            handleMergedId(track, resolvedId)
            android.util.Log.d(
                "MangaBakaTest",
                "updateLibManga: resolvedId=$resolvedId, track.remote_id=${track.remote_id}, track.last_chapter_read=${track.last_chapter_read}, track.score=${track.score}, private=${track.private}",
            )

            val nextRereads =
                if (track.toApiStatus() == "completed" && entry?.state == "rereading") {
                    (entry.numberOfRereads ?: 0) + 1
                } else {
                    entry?.numberOfRereads
                }

            val url = "$LIBRARY_API_URL/${track.remote_id}"
            val bodyJson = buildJsonObject {
                put("state", track.toApiStatus())
                put("is_private", track.private)
                if (track.last_chapter_read > 0.0) {
                    put("progress_chapter", track.last_chapter_read)
                } else {
                    put("progress_chapter", null)
                }
                if (track.score > 0) {
                    put("rating", track.score.toInt().coerceIn(0, 100))
                } else {
                    put("rating", null)
                }
                if (track.started_reading_date > 0) {
                    put("start_date", track.started_reading_date.toLocalDate().toString())
                } else {
                    put("start_date", null)
                }
                if (track.finished_reading_date > 0) {
                    put("finish_date", track.finished_reading_date.toLocalDate().toString())
                } else {
                    put("finish_date", null)
                }
                if (nextRereads != null) {
                    put("number_of_rereads", nextRereads)
                }
            }

            android.util.Log.d("MangaBakaTest", "updateLibManga: PUT $url body=$bodyJson")

            val body = bodyJson
                .toString()
                .toRequestBody()

            authClient
                .newCall(PUT(url, body = body, headers = headersOf("Content-Type", APP_JSON)))
                .awaitSuccess()

            libraryCache.remove(track.remote_id)
            seriesCache.remove(track.remote_id)

            track.title = seriesData.title
            track.total_chapters = seriesData.totalChapters?.toLongOrNull() ?: 0
            track
        }
    }

    suspend fun search(search: String): List<TrackSearch> {
        return withIOContext {
            val url = "$API_BASE_URL/v1/series/search".toUri().buildUpon()
                .appendQueryParameter("q", search)
                .appendQueryParameter("type_not", "novel")
                .build()
            with(json) {
                client.newCall(GET(url.toString()))
                    .awaitSuccess()
                    .parseAs<MangaBakaSearchResult>()
                    .data
                    .filter { it.state != "merged" }
                    .map { parseSearchItem(it) }
            }
        }
    }

    private fun parseSearchItem(item: MangaBakaItem): TrackSearch {
        return TrackSearch.create(trackId).apply {
            remote_id = item.mergedWith ?: item.id
            title = item.title
            summary = item.description.orEmpty().htmlDecode().trim()
            score = item.rating?.toBigDecimal()?.setScale(2, RoundingMode.HALF_UP)?.toDouble() ?: -1.0
            cover_url = item.cover.x350.x3.orEmpty()
            tracking_url = "$BASE_URL/${item.mergedWith ?: item.id}"
            total_chapters = item.totalChapters?.toLongOrNull() ?: 0
            start_date = item.year?.toString().orEmpty()
            publishing_status = item.status
            publishing_type = item.type.replaceFirstChar { c ->
                if (c.isLowerCase()) c.titlecase(Locale.getDefault()) else c.toString()
            }
            authors = item.authors.orEmpty()
            artists = item.artists.orEmpty()
        }
    }

    suspend fun getScoreStepSize(): Int {
        return withIOContext {
            with(json) {
                authClient.newCall(GET("$API_BASE_URL/v1/my/profile"))
                    .awaitSuccess()
                    .parseAs<MangaBakaUserProfileResponse>()
                    .data
                    .ratingSteps
            }
        }
    }

    suspend fun getAccessToken(code: String): MangaBakaOAuth {
        return withIOContext {
            val formBody = FormBody.Builder()
                .add("client_id", CLIENT_ID)
                .add("code", code)
                .add("code_verifier", codeVerifier)
                .add("code_challenge_method", "S256")
                .add("grant_type", "authorization_code")
                .add("redirect_uri", REDIRECT_URI)
                .add("scope", SCOPES)
                .build()

            with(json) {
                client.newCall(POST("${OAUTH_URL}/token", body = formBody))
                    .awaitSuccess().parseAs()
            }
        }
    }

    private data class CacheEntry<T>(val value: T, val cachedAt: Long = System.currentTimeMillis()) {
        fun isExpired() = System.currentTimeMillis() - cachedAt > CACHE_TTL_MS
    }

    private fun <K, V> lruCache(maxSize: Int): MutableMap<K, V> =
        Collections.synchronizedMap(
            object : LinkedHashMap<K, V>(maxSize + 1, 0.75f, true) {
                override fun removeEldestEntry(eldest: Map.Entry<K, V>) = size > maxSize
            }
        )

    private val seriesCache = lruCache<Long, CacheEntry<MangaBakaItem>>(MAX_CACHE_SIZE)
    private val libraryCache = lruCache<Long, CacheEntry<MangaBakaListEntry?>>(MAX_CACHE_SIZE)

    suspend fun fetchSeriesData(seriesId: Long): MangaBakaItem {
        return withIOContext {
            seriesCache[seriesId]?.takeUnless { it.isExpired() }?.value ?: run {
                val data = with(json) {
                    authClient.newCall(GET("$API_BASE_URL/v1/series/$seriesId"))
                        .awaitSuccess()
                        .parseAs<MangaBakaItemResult>()
                        .data
                }
                seriesCache[seriesId] = CacheEntry(data)
                data
            }
        }
    }

    private suspend fun fetchLibraryEntry(seriesId: Long): MangaBakaListEntry? {
        return withIOContext {
            libraryCache[seriesId]?.takeUnless { it.isExpired() }?.value ?: run {
                val entry = with(json) {
                    try {
                        authClient.newCall(GET("$LIBRARY_API_URL/$seriesId"))
                            .awaitSuccess()
                            .parseAs<MangaBakaListResult>()
                            .data
                    } catch (e: HttpException) {
                        if (e.code == 404) null else throw e
                    }
                }
                libraryCache[seriesId] = CacheEntry(entry)
                entry
            }
        }
    }

    private suspend fun handleMergedId(track: Track, resolvedId: Long) {
        track.remote_id = resolvedId
    }

    suspend fun resolveId(seriesId: Long): Long {
        val item = fetchSeriesData(seriesId)
        return item.mergedWith ?: item.id
    }

    suspend fun getMangaMetadata(track: DomainTrack): TrackMangaMetadata {
        return withIOContext {
            fetchSeriesData(track.remoteId).let {
                TrackMangaMetadata(
                    remoteId = it.mergedWith ?: it.id,
                    title = it.title,
                    thumbnailUrl = it.cover.raw.url,
                    description = it.description.orEmpty().htmlDecode().trim().ifEmpty { null },
                    authors = it.authors?.joinToString(", ")?.ifEmpty { null },
                    artists = it.artists?.joinToString(", ")?.ifEmpty { null },
                )
            }
        }
    }

    /**
     * Merges two library entries (original and resolved) by picking the best value
     * for each field:
     * - startDate: earliest non-null date (user started reading sooner)
     * - finishDate: latest non-null date (most recent completion)
     * - progressChapter: highest value (furthest read)
     * - rating: highest non-null value
     * - numberOfRereads: max of both (highest known reread count wins)
     * - state: most progressed status wins
     *   (completed > rereading > reading > paused > dropped > plan_to_read > considering)
     * - isPrivate: true if EITHER entry is private (privacy is never downgraded)
     *
     * When resolvedEntry is null (new merged series with no existing entry), returns
     * originalEntry unchanged so the caller can create a fresh entry from it.
     */
    private fun mergeBestEntry(
        originalEntry: MangaBakaListEntry,
        resolvedEntry: MangaBakaListEntry?,
    ): MangaBakaListEntry {
        if (resolvedEntry == null) return originalEntry

        val bestStartDate = listOfNotNull(originalEntry.startDate, resolvedEntry.startDate).minOrNull()
        val bestFinishDate = listOfNotNull(originalEntry.finishDate, resolvedEntry.finishDate).maxOrNull()
        val bestProgress = maxOf(
            originalEntry.progressChapter ?: 0.0,
            resolvedEntry.progressChapter ?: 0.0,
        ).takeIf { it > 0.0 }
        val bestRating = listOfNotNull(originalEntry.rating, resolvedEntry.rating).maxOrNull()
        val bestRereads = maxOf(originalEntry.numberOfRereads ?: 0, resolvedEntry.numberOfRereads ?: 0)

        val statusPriority = mapOf(
            "completed" to 7,
            "rereading" to 6,
            "reading" to 5,
            "paused" to 4,
            "dropped" to 3,
            "plan_to_read" to 2,
            "considering" to 1,
        )
        val bestState = if ((statusPriority[resolvedEntry.state] ?: 0) >= (statusPriority[originalEntry.state] ?: 0)) {
            resolvedEntry.state
        } else {
            originalEntry.state
        }

        val bestPrivate = originalEntry.isPrivate || resolvedEntry.isPrivate

        return resolvedEntry.copy(
            state = bestState,
            startDate = bestStartDate,
            finishDate = bestFinishDate,
            progressChapter = bestProgress,
            rating = bestRating,
            numberOfRereads = bestRereads,
            isPrivate = bestPrivate,
        )
    }

    companion object {
        private const val CLIENT_ID = "wOWYtfnAMjnornECeqIclcxOdUayYGqA"

        internal const val BASE_URL = "https://mangabaka.org"
        private const val API_BASE_URL = "https://api.mangabaka.dev"
        private const val LIBRARY_API_URL = "$API_BASE_URL/v1/my/library"
        private const val OAUTH_URL = "$BASE_URL/auth/oauth2"
        private const val SCOPES = "library.read library.write offline_access openid"

        private const val REDIRECT_URI = "komikku://mangabaka-auth"

        private const val APP_JSON = "application/json"

        private const val CACHE_TTL_MS = 20_000L
        private const val MAX_CACHE_SIZE = 100

        private var codeVerifier: String = ""

        fun authUrl(): Uri = "$OAUTH_URL/authorize".toUri().buildUpon()
            .appendQueryParameter("client_id", CLIENT_ID)
            .appendQueryParameter("code_challenge", getPkceS256ChallengeCode())
            .appendQueryParameter("code_challenge_method", "S256")
            .appendQueryParameter("response_type", "code")
            .appendQueryParameter("scope", SCOPES)
            .appendQueryParameter("redirect_uri", REDIRECT_URI)
            .build()

        fun refreshTokenRequest(token: String) = POST(
            "$OAUTH_URL/token",
            body = FormBody.Builder()
                .add("grant_type", "refresh_token")
                .add("client_id", CLIENT_ID)
                .add("refresh_token", token)
                .add("redirect_uri", REDIRECT_URI)
                .build(),
        )

        private fun getPkceS256ChallengeCode(): String {
            val codes = PkceUtil.generateS256Codes()
            codeVerifier = codes.codeVerifier
            return codes.codeChallenge
        }
    }
}
