package eu.kanade.tachiyomi.data.track.mangabaka

import android.net.Uri
import androidx.core.net.toUri
import eu.kanade.tachiyomi.data.database.models.Track
import eu.kanade.tachiyomi.data.track.TrackerManager
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaItem
import eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaItemResult
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
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Instant
import timber.log.Timber
import tachiyomi.domain.track.model.Track as DomainTrack

class MangaBakaApi(
    private val trackId: Long,
    private val client: OkHttpClient,
    interceptor: MangaBakaInterceptor,
) {
    private val json: Json by injectLazy()
    private val authClient = client.newBuilder().addInterceptor(interceptor).build()

    private data class CacheEntry<T>(val value: T, val expiresAt: Long)

    // In-memory TTL caches (thread-safe)
    private val mangaCache = ConcurrentHashMap<Long, CacheEntry<MangaBakaItem>>()
    private val libraryCache = ConcurrentHashMap<Long, CacheEntry<eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaListEntry>>()

    private fun now() = System.currentTimeMillis()

    suspend fun getMangaItem(seriesId: Long): MangaBakaItem {
        mangaCache[seriesId]?.let { entry ->
            if (now() < entry.expiresAt) {
                Timber.tag(TAG).d("getMangaItem: cache hit for $seriesId")
                return entry.value
            } else {
                Timber.tag(TAG).d("getMangaItem: cache expired for $seriesId")
                mangaCache.remove(seriesId)
            }
        }
        Timber.tag(TAG).d("GET series info: $API_BASE_URL/v1/series/$seriesId")
        val item = with(json) {
            authClient.newCall(GET("$API_BASE_URL/v1/series/$seriesId"))
                .awaitSuccess()
                .parseAs<MangaBakaItemResult>()
                .data
        }
        mangaCache[seriesId] = CacheEntry(item, now() + CACHE_TTL_MS)
        return item
    }

    suspend fun getLibraryEntry(seriesId: Long): eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaListEntry? {
        libraryCache[seriesId]?.let { entry ->
            if (now() < entry.expiresAt) {
                Timber.tag(TAG).d("getLibraryEntry: cache hit for $seriesId")
                return entry.value
            } else {
                Timber.tag(TAG).d("getLibraryEntry: cache expired for $seriesId")
                libraryCache.remove(seriesId)
            }
        }
        Timber.tag(TAG).d("GET library entry: $LIBRARY_API_URL/$seriesId")
        val entry = runCatching {
            with(json) {
                authClient.newCall(GET("$LIBRARY_API_URL/$seriesId"))
                    .awaitSuccess()
                    .parseAs<MangaBakaListResult>()
                    .data
            }
        }.getOrNull()
        if (entry != null) {
            libraryCache[seriesId] = CacheEntry(entry, now() + CACHE_TTL_MS)
        }
        return entry
    }

    suspend fun resolveId(seriesId: Long): Long {
        return withIOContext {
            val item = getMangaItem(seriesId)
            val resolved = item.mergedWith ?: item.id
            Timber.tag(TAG).d("resolveId: seriesId=$seriesId resolvedId=$resolved")
            resolved
        }
    }

    suspend fun addLibManga(track: Track): Track {
        return withIOContext {
            Timber.tag(TAG).d("addLibManga: input.remote_id=${track.remote_id}")
            val resolvedId: Long
            val seriesData: MangaBakaItem

            run {
                val result = getMangaItem(track.remote_id)
                resolvedId = result.mergedWith ?: result.id
                seriesData = result
                Timber.tag(TAG).d("addLibManga: resolvedId=$resolvedId")
            }

            if (resolvedId != track.remote_id) {
                Timber.tag(TAG).d("addLibManga: change track.remote_id from ${track.remote_id} to $resolvedId")
                track.remote_id = resolvedId
            }

            val url = "$LIBRARY_API_URL/${track.remote_id}"
            Timber.tag(TAG).d("addLibManga: POST $url")
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
            }.toString().toRequestBody()

            authClient
                .newCall(POST(url, body = body, headers = headersOf("Content-Type", APP_JSON)))
                .awaitSuccess()

            // seriesData loaded above
            track.title = seriesData.title
            track.total_chapters = seriesData.totalChapters?.toLongOrNull() ?: 0
            Timber.tag(TAG).d("addLibManga: set title=${track.title} total_chapters=${track.total_chapters}")
            track
        }
    }

    suspend fun deleteLibManga(track: DomainTrack) {
        withIOContext {
            Timber.tag(TAG).d("deleteLibManga: input.remoteId=${track.remoteId}")
            val resolvedId = resolveId(track.remoteId)
            val url = "$LIBRARY_API_URL/$resolvedId"
            Timber.tag(TAG).d("deleteLibManga: DELETE $url")
            authClient
                .newCall(DELETE(url))
                .awaitSuccess()
            // Invalidate cache
            libraryCache.remove(resolvedId)
        }
    }

    suspend fun findLibManga(track: Track): Track? {
        return withIOContext {
            with(json) {
                try {
                    val originalId = track.remote_id
                    Timber.tag(TAG).d("findLibManga: originalId=$originalId")

                    val entry = getLibraryEntry(originalId)
                    var additionalData: MangaBakaItem? = null
                    var resolvedId = originalId

                    val seriesResult = runCatching {
                        val item = getMangaItem(originalId)
                        additionalData = item
                        resolvedId = item.mergedWith ?: item.id
                        Timber.tag(TAG).d("findLibManga: series info resolvedId=$resolvedId")
                        item
                    }
                    if (seriesResult.isFailure && seriesResult.exceptionOrNull() is HttpException && (seriesResult.exceptionOrNull() as HttpException).code == 404) {
                        val mergeResult = runCatching {
                            Timber.tag(TAG).d("findLibManga: series not found, attempt resolveId")
                            val mergedId = resolveId(originalId)
                            resolvedId = mergedId
                            Timber.tag(TAG).d("findLibManga: resolved mergedId=$mergedId, GET $API_BASE_URL/v1/series/$mergedId")
                            val mergedItem = getMangaItem(mergedId)
                            additionalData = mergedItem
                            mergedItem
                        }
                        if (mergeResult.isFailure) throw mergeResult.exceptionOrNull()!!
                    }

                    if (resolvedId != originalId) {
                        runCatching {
                            Timber.tag(TAG).d("findLibManga: deleting old entry $LIBRARY_API_URL/$originalId")
                            authClient.newCall(DELETE("$LIBRARY_API_URL/$originalId")).awaitSuccess()
                        }
                        Timber.tag(TAG).d("findLibManga: update track.remote_id from $originalId to $resolvedId")
                        track.remote_id = resolvedId
                    }

                    if (entry != null && additionalData != null) {
                        Timber.tag(TAG).d("findLibManga: found entry and series, creating Track")
                        Track.create(TrackerManager.MANGABAKA).apply {
                            remote_id = resolvedId
                            title = additionalData!!.title
                            status = entry.getStatus()
                            score = entry.rating?.toDouble() ?: 0.0
                            started_reading_date = entry.startDate?.let { Instant.parse(it).toEpochMilliseconds() } ?: 0
                            finished_reading_date =
                                entry.finishDate?.let { Instant.parse(it).toEpochMilliseconds() } ?: 0
                            last_chapter_read = entry.progressChapter ?: 0.0
                            total_chapters = additionalData!!.totalChapters?.toLongOrNull() ?: 0
                            private = entry.isPrivate
                        }
                    } else {
                        Timber.tag(TAG).d("findLibManga: not found (null)")
                        null
                    }
                } catch (e: HttpException) {
                    if (e.code == 404) {
                        Timber.tag(TAG).d("findLibManga: not found (404)")
                        null
                    } else {
                        Timber.tag(TAG).e(e, "findLibManga: error")
                        throw e
                    }
                }
            }
        }
    }

    suspend fun updateLibManga(track: Track): Track {
        return withIOContext {
            val originalId = track.remote_id
            var resolvedId = originalId
            Timber.tag(TAG).d("updateLibManga: originalId=$originalId")

            var entry: eu.kanade.tachiyomi.data.track.mangabaka.dto.MangaBakaListEntry? = null

            entry = getLibraryEntry(originalId)

            val shouldResolve = entry == null
            if (shouldResolve) {
                resolvedId = resolveId(originalId)
                if (resolvedId != originalId) {
                    runCatching {
                        Timber.tag(TAG).d("updateLibManga: deleting old entry $LIBRARY_API_URL/$originalId")
                        authClient.newCall(DELETE("$LIBRARY_API_URL/$originalId")).awaitSuccess()
                    }
                    Timber.tag(TAG).d("updateLibManga: update track.remote_id from $originalId to $resolvedId")
                    track.remote_id = resolvedId
                }
            }

            val nextRereads = if (track.toApiStatus() == "completed" && entry?.state == "rereading") {
                (entry.numberOfRereads ?: 0) + 1
            } else {
                entry?.numberOfRereads
            }

            val url = "$LIBRARY_API_URL/${track.remote_id}"
            val body = buildJsonObject {
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
            }.toString().toRequestBody()

            Timber.tag(TAG).d("updateLibManga: PUT $url")
            authClient
                .newCall(PUT(url, body = body, headers = headersOf("Content-Type", APP_JSON)))
                .awaitSuccess()
            // Invalidate cache as entry is now updated
            libraryCache.remove(track.remote_id)

            val latestData = runCatching {
                Timber.tag(TAG).d("updateLibManga: GET (final info) $API_BASE_URL/v1/series/${track.remote_id}")
                getMangaItem(track.remote_id)
            }.getOrNull()
            if (latestData != null) {
                track.title = latestData.title
                track.total_chapters = latestData.totalChapters?.toLongOrNull() ?: 0
                Timber.tag(TAG).d("updateLibManga: set title=${track.title} total_chapters=${track.total_chapters}")
            }
            track
        }
    }

    suspend fun search(search: String): List<TrackSearch> {
        return withIOContext {
            val url = "$API_BASE_URL/v1/series/search".toUri().buildUpon()
                .appendQueryParameter("q", search)
                .appendQueryParameter("type_not", "novel")
                .build()
            Timber.tag(TAG).d("search: $url")
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
            start_date = item.year?.toString().orEmpty()
            publishing_status = item.status
            publishing_type = item.type.replaceFirstChar { c ->
                if (c.isLowerCase()) c.titlecase(Locale.getDefault()) else c.toString()
            }
            authors = item.authors.orEmpty()
            artists = item.artists.orEmpty()
            total_chapters = item.totalChapters?.toLongOrNull() ?: 0
        }
    }

    suspend fun getMangaMetadata(track: DomainTrack): TrackMangaMetadata {
        return withIOContext {
            with(json) {
                val resolvedId = resolveId(track.remoteId)
                Timber.tag(TAG).d("getMangaMetadata: GET series info $API_BASE_URL/v1/series/$resolvedId")
                val item = getMangaItem(resolvedId)
                TrackMangaMetadata(
                    remoteId = item.id,
                    title = item.title,
                    thumbnailUrl = item.cover.raw.url,
                    description = item.description.orEmpty().htmlDecode().trim().ifEmpty { null },
                    authors = item.authors?.joinToString(", ")?.ifEmpty { null },
                    artists = item.artists?.joinToString(", ")?.ifEmpty { null },
                )
            }
        }
    }

    suspend fun getScoreStepSize(): Int {
        return withIOContext {
            with(json) {
                Timber.tag(TAG).d("getScoreStepSize: GET $API_BASE_URL/v1/my/profile")
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
            Timber.tag(TAG).d("getAccessToken: code=$code")
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
                Timber.tag(TAG).d("getAccessToken: POST ${OAUTH_URL}/token")
                client.newCall(POST("${OAUTH_URL}/token", body = formBody))
                    .awaitSuccess().parseAs()
            }
        }
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

        private var codeVerifier: String = ""

        /** Timber Tag for logcat filtering */
        private const val TAG = "mangabaka"

        private const val CACHE_TTL_MS = 60_000L // 1 minute

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
