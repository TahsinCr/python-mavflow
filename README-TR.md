[![Sürüm: v1.0.0][version-shield]](https://github.com/TahsinCr/python-mavflow/blob/main/CHANGELOG.md)
[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![MIT Lisansı][license-shield]](https://github.com/TahsinCr/python-mavflow/blob/main/LICENSE)

[English](https://github.com/TahsinCr/python-mavflow/blob/main/README.md) | **Türkçe**

# Mavflow

[Depo](https://github.com/TahsinCr/python-mavflow) · [Sürümler](https://github.com/TahsinCr/python-mavflow/releases) · [Kaynak MAVLink paketi](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink) · [MIT Lisansı](https://github.com/TahsinCr/python-mavflow/blob/main/LICENSE)

Mavflow; Python uygulamaları için MAVLink endpoint ve bağlantıları, mesaj
yönlendirme, telemetri abonelikleri, sınırlı asyncio teslimatı ve isteğe bağlı
uygulama paketi protokolü sağlar.

Kütüphane, [Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core)
içindeki MAVLink katmanından ayrıştırılmıştır. Mavflow bağımsız bir kütüphane
olarak geliştirilir; kurulması ve kullanılması için kaynak depoya ihtiyaç
duyulmaz. Transport, yönlendirme, uygulama protokolü, testler ve örnekler bu
depoda yer alır.

Mavflow yeniden kullanılabilir MAVLink altyapısına odaklanır. Araç görevleri
ve projeye özel uygulama davranışları, bu kütüphaneyi kullanan projelere aittir.

## İçindekiler

- [Başlangıç](#getting-started)
  - [Kurulum](#install)
  - [Sürüm](#version)
  - [İlk heartbeat mesajını alma](#first-heartbeat)
  - [API katmanını seçme](#choose-api-layer)
- [Temel kavramlar](#core-concepts)
  - [Endpoint ve bağlantılar](#endpoints)
  - [Tek okuyucu ve mesaj router'ı](#router)
  - [Filtreleme ve mesaj geçmişi](#filtering)
  - [Yaşam döngüsü ve bileşim](#lifecycle)
  - [Event teslimatı](#event-delivery)
- [Telemetri ile çalışma](#telemetry)
  - [Mesajlara abone olma](#subscribe)
  - [Mesaj bekleme ve geçmişi inceleme](#wait-and-history)
- [Async uygulamalar](#async-applications)
- [Uygulama paketleri](#application-packets)
  - [Paket kodlama ve birleştirme](#codec)
  - [Eşler, istekler ve handler'lar](#peers-and-requests)
  - [Uzak log modelleri](#remote-logs)
- [Güvenlik ve çalışma sınırları](#operational-boundaries)
- [Yığını birleştirme](#composition)
- [Yaşam döngüsü ve başvuru](#reference)
  - [Varsayılanlar](#defaults)
  - [Bileşen sorumlulukları](#components)
  - [Public tipler ve hatalar](#public-types)
- [Geliştirme ve lisans](#development)

<a id="getting-started"></a>
## Başlangıç

<a id="install"></a>
### Kurulum

Python 3.10 veya yenisi gerekir. Kaynak checkout içinden Mavflow'u etkin
ortama kurun:

~~~bash
python -m pip install .
~~~

PyPI sürümü yayımlandıktan sonra dağıtımı şu şekilde kurabilirsiniz:

~~~bash
python -m pip install mavflow
~~~

Dağıtımın ve Python import paketinin adı `mavflow`'dur. Zorunlu bağımlılıklar
olan `flowevent` ve `pymavlink` kurulum sırasında yüklenir. `flowevent` event
bus ve abonelik handle'larını sağlar; Mavflow bu tipleri doğrudan kullanır.

Düzenlenebilir geliştirme kurulumu ve araçları için:

~~~bash
python -m pip install -e ".[dev]"
~~~

`dev` eklentisi build, coverage, pyperf, Pyright, Ruff ve Twine araçlarını
sağlar.

<a id="version"></a>
### Sürüm

Kurulu paket sürümüne `mavflow.__version__` üzerinden erişebilirsiniz:

~~~python
from mavflow import __version__

print(__version__)  # 1.0.0
~~~

<a id="first-heartbeat"></a>
### İlk heartbeat mesajını alma

Bu örnek UDP 14550 portunu dinler, bir heartbeat bekler ve sonraki heartbeat
mesajlarını gözlemler. Araç veya simülatörün yapılandırılan endpoint'e
MAVLink paketi göndermesi gerekir.

~~~python
from mavflow import MavlinkEndpoint, MavlinkRuntime


def heartbeat_goster(message: object) -> None:
    print(message)


endpoint = MavlinkEndpoint.udp("0.0.0.0", 14550)
with MavlinkRuntime(endpoint) as link:
    abonelik = link.subscribe("HEARTBEAT", heartbeat_goster)
    try:
        ilk = link.wait_for("HEARTBEAT", timeout=10.0)
        print("İlk heartbeat:", ilk)
    finally:
        abonelik.cancel()
~~~

Context açıldığında runtime bağlantı ve router'ı başlatır; context kapanınca
ikisini de durdurur. `wait_for()` ilk eşleşen mesajı döndürür, süre aşımında
`None` verir. Komut satırından çalıştırılabilen sürüm için
[examples/telemetry.py](examples/telemetry.py) dosyasına bakın.

<a id="choose-api-layer"></a>
### API katmanını seçme

| Bileşen | Kullanım amacı |
| --- | --- |
| `MavlinkConnection` | `pymavlink` transportunu açmak ve ham MAVLink mesajlarını gönderip almak. |
| `MavlinkMessageRouter` | Okuma döngüsünü sahiplenmek, mesajları filtrelemek ve sınırlı geçmiş tutmak. |
| `MavlinkClient` | Bağlantı ve router'ı tek bir senkron fasad üzerinden kullanmak. |
| `MavlinkRuntime` | Client yaşam döngüsünü ve isteğe bağlı uygulama eşleriyle handler'ları yönetmek. |
| `MavlinkAsyncChannel` | Yönlendirilen mesajları sınırlı kuyruk üzerinden asyncio event loop'una iletmek. |

Normal uygulama giriş noktası olarak `MavlinkRuntime` kullanın. Uygulamanın
transport veya router sahipliğini kendisinin yönetmesi gerekiyorsa alt
katmanları doğrudan birleştirin.

<a id="core-concepts"></a>
## Temel kavramlar

<a id="endpoints"></a>
### Endpoint ve bağlantılar

`MavlinkEndpoint` değiştirilemez bir bağlantı tanımıdır. `tcp(host, port)`,
`udp(host, port)` ve `serial(device, baud=...)` yardımcıları yaygın endpoint
biçimlerini oluşturur:

~~~python
from mavflow import MavlinkEndpoint

udp = MavlinkEndpoint.udp("0.0.0.0", 14550)
tcp = MavlinkEndpoint.tcp("192.0.2.10", 5760)
serial = MavlinkEndpoint.serial("/dev/ttyUSB0", baud=57600)
~~~

Endpoint ayrıca yerel system ve component kimliklerini, MAVLink dialect'ini,
yeniden bağlanma ayarını ve heartbeat timeout değerini taşır. Varsayılan URI
`udp:127.0.0.1:14550`'dir. Araç, radyo, simülatör veya router tarafından
beklenen adresi ve transport biçimini seçin.

`MavlinkConnection`, alttaki `pymavlink` bağlantısının sahibidir. `mavflow`
import edildiğinde bir cihaz açılmaz ve transport modülü yüklenmez; `pymavlink`
gerçek bağlantı oluşturulurken yüklenir. Connection; yaşam döngüsü metotlarını,
ham mesaj erişimini, gönderim yardımcılarını ve araç hedef bilgilerini sağlar.

<a id="router"></a>
### Tek okuyucu ve mesaj router'ı

`MavlinkMessageRouter` bir bağlantının mesaj okuma döngüsünü sahiplenir.
Gelen her mesajı bir kez okur, mesaj ve sıra durumunu kaydeder, ardından
eşleşen mesajları etkin aboneliklere teslim eder.

Tek okuyucu sahipliği transport okumasını ve yönlendirmeyi tek yerde tutar.
Router callback'leri mesaj teslimatı sırasında çalışır; bu nedenle kısa
tutulmaları gerekir. Okuma döngüsünü bekletmemek için bloklayan işleri
uygulamanın kendi worker'larına taşıyın.

Router güncel istatistikleri, eşleşen son mesajı, sınırlı geçmişi ve mesaj
bekleme işlemini sunar. `MavlinkClient` bir connection ile router'ı bir araya
getirir; uygulamanın bu nesneleri elle bağlamasını gerektirmez.

<a id="filtering"></a>
### Filtreleme ve mesaj geçmişi

`MavlinkMessageFilter` mesaj tiplerine, kaynak system kimliklerine, kaynak
component kimliklerine, mesaj ID'lerine, mesaja uygulanan bir koşula veya
predicate'e göre eşleşme yapabilir. Callback yalnızca istenen telemetri
alt kümesini alacak şekilde kriterleri birleştirin:

~~~python
from mavflow import MavlinkMessageFilter

konum_filtresi = MavlinkMessageFilter.for_types(
    "GLOBAL_POSITION_INT",
    source_systems=1,
)
~~~

Callback kaydı için filtreyi `subscribe()` metoduna verin. Tip filtresi
doğrudan `wait_for()`, `latest()` veya router'ın `history()` metoduna da
verilebilir. Yalnızca en yeni eşleşen mesajları almak için
`history()` ile `limit` kullanın. Geçmiş bellekte tutulur ve sınırlıdır; kalıcı
bir telemetri günlüğü değildir.

<a id="lifecycle"></a>
### Yaşam döngüsü ve bileşim

`MavlinkRuntime` bir client'ı ve istendiğinde uygulama channel'ını, peer'i ve
dispatcher'ı bir araya getirir. `start()` transportu ve sahip olunan uygulama
servislerini bağımlılık sırasına göre başlatır. `stop()` ters sırayla kapatır
ve kapatma hatalarını toplar. İlk örnekteki gibi senkron context manager olarak
da kullanılabilir.

Ayrı bağlantılar için ayrı runtime'lar oluşturun. Daha alt seviyeli connection
ve router'ı paylaşması veya yapılandırması gereken uygulamalar mevcut bir
`MavlinkClient` nesnesini runtime'a verebilir.

<a id="event-delivery"></a>
### Event teslimatı

Mavflow event bus ve abonelik handle'ları için `flowevent` kullanır. Runtime
mesaj, paket ve hata event'leri bu kütüphanenin sözleşmelerini doğrudan
kullanır; Mavflow ikinci bir event bus uygulamaz.

`subscribe()` veya `handle()` tarafından döndürülen abonelikler açık bir
`cancel()` yaşam döngüsüne sahiptir. İş tamamlandığında aboneliği iptal edin
veya onu ilgili servis yaşam döngüsüyle yönetin.

<a id="telemetry"></a>
## Telemetri ile çalışma

<a id="subscribe"></a>
### Mesajlara abone olma

`MavlinkRuntime.subscribe()` bir mesaj tipini veya filtreyi ve callback'i
kabul eder. Dönen abonelik runtime'dan bağımsız olarak iptal edilebilir:

~~~python
from mavflow import MavlinkEndpoint, MavlinkMessageFilter, MavlinkRuntime


def konumu_goster(message: object) -> None:
    print(message)


konum_filtresi = MavlinkMessageFilter.for_types(
    "GLOBAL_POSITION_INT",
    source_systems=1,
)

with MavlinkRuntime(MavlinkEndpoint.udp("0.0.0.0", 14550)) as link:
    konum_aboneligi = link.subscribe(konum_filtresi, konumu_goster)
    heartbeat_aboneligi = link.subscribe("HEARTBEAT", print)
    try:
        link.wait_for("HEARTBEAT", timeout=10.0)
    finally:
        konum_aboneligi.cancel()
        heartbeat_aboneligi.cancel()
~~~

Mesaj filtreleri kaynak component ve mesaj ID'siyle de sınırlandırılabilir;
alınan mesaja bir koşul veya predicate uygulanabilir. Mesaj nesnesi
`pymavlink` tarafından sağlanır. Kaynak system, kaynak component ve mesaj tipi
gibi alanlar için üretilen mesaj erişim metotlarını kullanın.

<a id="wait-and-history"></a>
### Mesaj bekleme ve geçmişi inceleme

Kodun tek bir eşleşen mesaja ihtiyacı varsa `wait_for()` kullanın. Daha önce
gelen en yeni eşleşmeyi `latest()` ile, saklanan mesajları `history()` ile
alabilirsiniz:

~~~python
from mavflow import MavlinkEndpoint, MavlinkRuntime

with MavlinkRuntime(MavlinkEndpoint.udp("0.0.0.0", 14550)) as link:
    ilk_heartbeat = link.wait_for("HEARTBEAT", timeout=10.0)
    son_mesaj = link.latest("HEARTBEAT")
    son_mesajlar = link.router.history("HEARTBEAT", limit=20)
    router_durumu = link.state.router
~~~

`wait_for()` süre aşımında `None` döndürür. Eşleşen mesaj henüz alınmadıysa
`latest()` de `None` verir. History, mesaj envelope'larını yayın sırasıyla
tutar. Router istatistikleri alınan mesajları, okuma ve dispatch hatalarını,
tahmini kayıp mesajları ve teslimat kalitesini içerir.

Client veya runtime üzerinden MAVLink mesajı göndermek için `send()` ya da
`send_named()` kullanın. Metot adı ve argümanlar üretilen `pymavlink`
dialect API'sini izler.

<a id="async-applications"></a>
## Async uygulamalar

MAVLink transport okuması senkron router'ın sahipliğinde kalır.
`MavlinkAsyncChannel` seçilen mesajları bir asyncio event loop'una aktarır.
Channel sınırlı bir kuyruk kullanır; kuyruk dolduğunda en eski bekleyen mesajı
atar ve `dropped_messages` sayacını artırır.

Channel'ı çalışan event loop içinde başlatın. Router aboneliğini düzgün
kapatabilmesi için runtime'ı durdurmadan önce channel'ı durdurun:

~~~python
import asyncio

from mavflow import MavlinkAsyncChannel, MavlinkEndpoint, MavlinkRuntime


async def main() -> None:
    runtime = MavlinkRuntime(MavlinkEndpoint.udp("0.0.0.0", 14550))
    channel = MavlinkAsyncChannel(
        runtime.router,
        "HEARTBEAT",
        maxsize=64,
    )
    channel.start()
    try:
        runtime.start()
        message = await channel.receive(timeout=10.0)
        print(message)
    finally:
        channel.stop()
        runtime.stop()


asyncio.run(main())
~~~

Süre dolmadan mesaj gelmezse `receive(timeout=...)` timeout hatası yükseltir.
`receive_nowait()` kuyruktaki hazır mesajı alır; kuyruk boşsa
`asyncio.QueueEmpty` yükseltir. Bekleyen ve atılan mesaj sayaçları sınırlı
teslimatın izlenmesini sağlar.

<a id="application-packets"></a>
## Uygulama paketleri

İsteğe bağlı uygulama protokolü JSON uyumlu Mavflow paketlerini MAVLink
`V2_EXTENSION` mesajları üzerinden taşır. Bu protokol standart telemetri
yönlendirmesinden ayrıdır; yalnızca normal MAVLink mesajlarına ihtiyaç duyan
uygulamaların etkinleştirmesi gerekmez.

<a id="codec"></a>
### Paket kodlama ve birleştirme

`MavlinkApplicationPacket` paket tipi, JSON uyumlu payload, paket ID'si,
zaman damgası, kaynak kimlikleri ve yanıt bayrağını taşır.
`MavlinkApplicationCodec` paketi sınırlandırılmış parçalara böler, checksum
ekler ve decode sırasında parçaları doğrular. `MavlinkApplicationAssembler`
eksik paketlerin sayısını, belleğini ve yaşam süresini sınırlandırarak
parçaları birleştirir.

Codec araç bağlantısı açılmadan da kullanılabilir:

~~~python
from mavflow import (
    MavlinkApplicationAssembler,
    MavlinkApplicationCodec,
    MavlinkApplicationPacket,
)

packet = MavlinkApplicationPacket(
    "vehicle.health",
    {"ready": True, "battery_percent": 84},
    packet_id=42,
)
parcalar = MavlinkApplicationCodec.encode(packet)

assembler = MavlinkApplicationAssembler()
cozulmus = None
for parca in parcalar:
    cozulmus = assembler.accept(parca, source_system=1, source_component=1)

if cozulmus is None:
    raise RuntimeError("Paket bütünüyle birleştirilemedi")

print(cozulmus.to_dict())
~~~

Tam örnek için [examples/application_packets.py](examples/application_packets.py)
dosyasına bakın. Codec hatalı JSON, geçersiz checksum, desteklenmeyen protokol
sürümü ve yapılandırılan sınırları aşan paketleri reddeder.

<a id="peers-and-requests"></a>
### Eşler, istekler ve handler'lar

Runtime oluştururken `application_role` vererek uygulama channel'ını, peer'i
ve dispatcher'ı etkinleştirin:

~~~python
from mavflow import MavlinkEndpoint, MavlinkRuntime


def saglik_bilgisi(_request: object) -> dict[str, object]:
    return {"ready": True, "battery_percent": 84}


endpoint = MavlinkEndpoint.udp("0.0.0.0", 14550)
runtime = MavlinkRuntime(endpoint, application_role="vehicle")
handler = runtime.handle("vehicle.health.query", saglik_bilgisi)

try:
    with runtime:
        # Uzak peer artık vehicle.health.query isteği gönderebilir.
        ...
finally:
    handler.cancel()
~~~

Handler bir `MavlinkApplicationPacket` alır. Bir mapping,
`MavlinkApplicationResult` veya `None` döndürebilir. İstek yanıt bekliyorsa
Mavflow handler sonucunu isteği gönderen peer'a iletir. Handler kaydı iptal
edilebilir bir abonelik döndürür.

Tek yönlü paket için `notify()`, ilişkili bir yanıt beklendiğinde `request()`
kullanın. İstek bir ID taşır, yanıt tipi filtresi kabul eder ve süre dolmadan
eşleşen yanıt gelmezse `TimeoutError` yükseltir:

~~~python
with MavlinkRuntime(endpoint, application_role="ground-station") as link:
    response = link.request(
        "vehicle.health.query",
        {},
        response_types="system.ack",
        timeout=3.0,
    )
    print(response.response.payload)
~~~

Varsayılan yanıt tipleri `system.ack` ve `system.error`'dur. Peer ayrıca uzak
canlılığı ve transport kullanılabilirliğini izler. İstek zaman aşımını ve
dispatcher kapasitesini uygulamanın çalışma yüküne göre ayarlayın.

<a id="remote-logs"></a>
### Uzak log modelleri

`MavlinkRemoteLogRecord` ve `MavlinkRemoteLogBatch` uygulama log kayıtlarını
tanımlar ve protokol payload'larına dönüştürülebilir. Bu tipler doğrulanmış
kayıt ve batch modelleri sağlar; Python logging handler'ı yapılandırmaz veya
logları yerel depolamaya yazmaz.

<a id="operational-boundaries"></a>
## Güvenlik ve çalışma sınırları

Mavflow MAVLink transportu ve yeniden kullanılabilir yönlendirme altyapısı
sağlar. Araç politikasına, komut yetkilendirmesine, görev davranışına veya
uygulamanın telemetriyi nasıl saklayacağına karar vermez.

Router history'si, asyncio channel'ı, uygulama parçası birleştirme belleği ve
dispatcher işleri bellekte tutulur ve sınırlıdır. Kendi sahip oldukları yaşam
döngüsü sona erdiğinde temizlenir; kalıcı kuyruk değildir. Uzun çalışan
servislerde router istatistiklerini, channel drop sayısını, peer durumunu ve
runtime/uygulama hata event'lerini izleyin.

MAVLink endpoint'leri ve ağ erişimi araç ile dağıtım yapılandırmasına uygun
olmalıdır. Transport güvenliği, komut politikası ve iş düzeyindeki payload
doğrulaması uygulamanın sorumluluğundadır.

<a id="composition"></a>
## Yığını birleştirme

Temel telemetri yolu `MavlinkEndpoint` → `MavlinkConnection` →
`MavlinkMessageRouter` → `MavlinkClient` → `MavlinkRuntime` şeklindedir.
Her katmanın tek bir işi vardır: adresi tanımlamak, transportu sahiplenmek,
okuma döngüsünü yönetmek, senkron bir fasad sağlamak ve uygulama yaşam
döngüsünü koordine etmek.

Uygulama rolü aynı client ve router üzerine paket channel'ı, peer durumu ve
sınırlı dispatcher ekler:

~~~python
from mavflow import MavlinkClient, MavlinkEndpoint, MavlinkRuntime

endpoint = MavlinkEndpoint.udp("0.0.0.0", 14550)
client = MavlinkClient(endpoint)
runtime = MavlinkRuntime(
    client=client,
    application_role="ground-station",
    workers=2,
    max_pending=128,
)

with runtime:
    runtime.notify("mission.status", {"state": "ready"})
~~~

Transport ve router ortak kalır. Uygulama rolü eklemek aynı bağlantı için
ikinci bir okuma döngüsü başlatmaz. Bağımsız transport veya yaşam döngüsü
gerekiyorsa ayrı runtime kullanın.

<a id="reference"></a>
## Yaşam döngüsü ve başvuru

<a id="defaults"></a>
### Varsayılanlar

| Ayar | Varsayılan | Açıklama |
| --- | --- | --- |
| Endpoint URI | `udp:127.0.0.1:14550` | Yerel varsayılan endpoint. |
| Endpoint system/component | `255 / 0` | Yerel MAVLink kaynak kimlikleri. |
| Endpoint dialect | `ardupilotmega` | `pymavlink` tarafından kullanılır. |
| Heartbeat timeout | `10 saniye` | Bağlantı ayarı. |
| Async channel kapasitesi | `128 mesaj` | Taşmada en eski bekleyen mesaj atılır. |
| Application role | Kapalı | Peer ve dispatcher oluşturmak için `application_role` verin. |
| Application dispatcher | `1 worker, 64 bekleyen iş` | Application role etkinleştirildiğindeki runtime varsayılanı. |

<a id="components"></a>
### Bileşen sorumlulukları

| Bileşen | Sorumluluk |
| --- | --- |
| `MavlinkEndpoint` | TCP, UDP veya seri bağlantı tanımını normalize etmek. |
| `MavlinkConnection` | `pymavlink` transportu, yaşam döngüsünü ve ham gönderme/alma işlemlerini yönetmek. |
| `MavlinkMessageRouter` | Tek okuma döngüsünü, filtreli callback'leri, sıra durumunu ve sınırlı geçmişi yönetmek. |
| `MavlinkClient` | Connection ile router'ı senkron bir fasad olarak birleştirmek. |
| `MavlinkRuntime` | Client ve isteğe bağlı uygulama servislerini birlikte başlatıp durdurmak. |
| `MavlinkAsyncChannel` | Filtreli router teslimatını sınırlı asyncio kuyruğuna aktarmak. |
| `MavlinkApplicationChannel` | Uygulama parçalarını MAVLink `V2_EXTENSION` üzerinden taşımak. |
| `MavlinkApplicationPeer` | Bildirim ve ilişkili istekleri göndermek, peer canlılığını izlemek. |
| `MavlinkApplicationDispatcher` | Kayıtlı paket handler'larını sınırlı worker kapasitesiyle çalıştırmak. |

### Runtime metotları

| Metot | Davranış |
| --- | --- |
| `start()` / `stop()` | Runtime'ı ve sahip olduğu servisleri başlatıp durdurmak. |
| `subscribe(filter, callback)` | Eşleşen MAVLink mesajlarını senkron callback'e yönlendirmek. |
| `once(filter, callback)` | Tek eşleşen mesaj için callback kaydetmek. |
| `wait_for(filter, timeout=None)` | Eşleşen bir mesajı veya süre dolunca `None` döndürmek. |
| `latest(filter=None)` | En yeni eşleşen mesajı veya `None` döndürmek. |
| `notify(packet_type, payload=None)` | Tek yönlü uygulama paketi göndermek; application role gerekir. |
| `request(packet_type, payload=None, ...)` | İlişkili uygulama isteği göndermek ve yanıt beklemek. |
| `handle(packet_type, handler)` | Uygulama handler'ı kaydetmek; iptal edilebilir abonelik döndürür. |

<a id="public-types"></a>
### Public tipler ve hatalar

Paket aşağıdaki public isimleri dışa aktarır:

| Alan | Public isimler |
| --- | --- |
| Transport ve yönlendirme | `MavlinkEndpoint`, `MavlinkConnection`, `MavlinkMessageRouter`, `MavlinkMessageFilter`, `MavlinkMessageEnvelope`, `MavlinkHeader`, `MavlinkMessage`, `MavlinkRouterStats`, `MessageCache` |
| Runtime ve async | `MavlinkClient`, `MavlinkRuntime`, `MavlinkRuntimeState`, `MavlinkAsyncChannel` |
| Uygulama protokolü | `MavlinkApplicationPacket`, `MavlinkApplicationCodec`, `MavlinkApplicationAssembler`, `MavlinkApplicationChannel`, `MavlinkApplicationPeer`, `MavlinkApplicationDispatcher`, `MavlinkApplicationHandlerRegistry`, `MavlinkApplicationResult`, `MavlinkApplicationResponse` |
| Uzak log | `MavlinkRemoteLogLevel`, `MavlinkRemoteLogRecord`, `MavlinkRemoteLogBatch` |

Önemli hata tipleri arasında `MavlinkUnavailableError`,
`MavlinkRouterError`, `MavlinkRuntimeError` ve
`MavlinkApplicationProtocolError` bulunur. Uygulamanın özel olarak işlemesi
gereken hata sınıflarını `mavflow` paketinden import edin.

<a id="development"></a>
## Geliştirme ve lisans

Proje Python 3.10 ve yenisini destekler, build backend olarak setuptools
kullanır. Geliştirme bağımlılıklarını kurup kontrolleri proje kökünden
çalıştırın:

~~~bash
python -m pip install -e ".[dev]"
python run_tests.py
python -m coverage run --branch -m unittest discover -s tests -v
python -m coverage report --fail-under=75
python -m ruff check .
python -m ruff format --check .
python -m pyright
python -m build
python -m twine check dist/*
~~~

Test paketi gerçek `pymavlink` UDP loopback kontrolünü içerir. Yalnızca bu
testi çalıştırmak için `python -m unittest tests.test_pymavlink_integration -v`
komutunu kullanın.

Benchmark paketi kalibrasyon, worker süreçleri ve JSON sonuç dosyaları için
[pyperf](https://pyperf.readthedocs.io/) kullanır. `dev` eklentisini kurduktan
sonra benchmark modülünü proje kökünden çalıştırın:

~~~bash
python -m benchmarks.benchmark_router --output router-run.json
python -m benchmarks.benchmark_codec --output codec-run.json
python -m pyperf show --metadata --stats router-run.json
python -m pyperf compare_to --table benchmarks/results/router.json router-run.json
~~~

Her çalıştırma için kullanılmamış bir çıktı dosyası seçin; pyperf mevcut
dosyanın üzerine yazmaz. Router benchmark'ı önceden doldurulmuş bellek içi
connection üzerinden tek okuyuculu teslimatı ölçer. Codec benchmark'ı
uygulama paketi kodlama ve birleştirmeyi ölçer. `--fast` yalnız CI smoke
kontrolleri içindir; yerel ölçümlerde varsayılan kalibrasyonu kullanın.
Sonuçlar kaydedilen yorumlayıcıya, platforma, kaynak fingerprint'ine ve iş
yüküne özgüdür; canlı sistem performansı garantisi değildir. İş yükleri ve
karşılaştırma notları için [benchmarks/results/README.md](benchmarks/results/README.md)
dosyasına bakın.

`CHANGELOG` ayrıştırma kaynağı olan
[Vehicle Autonomy Core MAVLink paketini](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink)
kaydeder.

Telif © 2026 TahsinCr. Mavflow MIT Lisansı ile dağıtılır. Ayrıntılar için
[LICENSE](https://github.com/TahsinCr/python-mavflow/blob/main/LICENSE) dosyasına bakın.

[version-shield]: https://img.shields.io/badge/version-v1.0.0-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
