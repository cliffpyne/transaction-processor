package com.elegansky.smsrescue.net

import com.squareup.moshi.JsonClass
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Response
import retrofit2.Retrofit
import retrofit2.converter.moshi.MoshiConverterFactory
import retrofit2.http.Body
import retrofit2.http.POST
import java.util.concurrent.TimeUnit

@JsonClass(generateAdapter = true)
data class SmsRescueRequest(
    val message: String,
    val sender: String? = null,
    /** ISO-8601 UTC timestamp of when the phone received the SMS. */
    val received_at: String? = null,
)

@JsonClass(generateAdapter = true)
data class SmsRescueResponse(
    val rescued: Boolean = false,
    val row_id: Long? = null,
    val source_tab: String? = null,
    val plate: String? = null,
    val ref: String? = null,
    val error: String? = null,
    val message: String? = null,
)

interface SmsRescueApi {
    @POST("api/sms-rescue")
    suspend fun rescue(@Body body: SmsRescueRequest): Response<SmsRescueResponse>
}

object ApiFactory {

    /**
     * Build a Retrofit client.
     *
     * fastMode=true is for calls that run inside a BroadcastReceiver.goAsync()
     * window. Android kills any receiver that blocks longer than 10 s with an
     * ANR — under Samsung's background-throttled state a POST can take 20+ s
     * to even establish a TCP connection, so the receiver ANRs on nearly every
     * SMS_DELIVER. We use aggressive 3 s connect / 5 s read here and let
     * anything that misses that budget fail fast; SmsWorker then picks up the
     * queued row later with the slow timeouts and retries at its leisure.
     *
     * fastMode=false is for WorkManager drain calls — no ANR limit, keep the
     * generous 20 s / 30 s timeouts to survive genuine bad-network moments.
     */
    fun build(baseUrl: String, token: String, fastMode: Boolean = false): SmsRescueApi {
        val moshi = Moshi.Builder().add(KotlinJsonAdapterFactory()).build()
        val tokenInterceptor = Interceptor { chain ->
            val req = chain.request().newBuilder()
                .addHeader("X-Migration-Token", token)
                .build()
            chain.proceed(req)
        }
        val (connectSec, readSec) = if (fastMode) 3L to 5L else 20L to 30L
        val client = OkHttpClient.Builder()
            .addInterceptor(tokenInterceptor)
            .addInterceptor(HttpLoggingInterceptor().apply {
                level = HttpLoggingInterceptor.Level.BASIC
            })
            .retryOnConnectionFailure(!fastMode)
            .connectTimeout(connectSec, TimeUnit.SECONDS)
            .readTimeout(readSec, TimeUnit.SECONDS)
            .writeTimeout(readSec, TimeUnit.SECONDS)
            .build()
        return Retrofit.Builder()
            .baseUrl(baseUrl.trimEnd('/') + "/")
            .client(client)
            .addConverterFactory(MoshiConverterFactory.create(moshi))
            .build()
            .create(SmsRescueApi::class.java)
    }
}
