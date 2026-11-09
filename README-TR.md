[![Sürüm: v2.0][version-shield]](https://github.com/TahsinCr/python-mavflow/blob/main/CHANGELOG.md)
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
  - [Bir örneği çalıştırma](#run-an-example)
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
- [Sorun giderme](#troubleshooting)
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

Bu kırıcı API serisi **v2.0**'dır. Mesaj aboneliği, bekleme, filtreleme ve
saklanan geçmiş `runtime.messages` üzerinden doğrudan EventFlow kullanır;
Mavflow paralel bir abonelik veya history API'si tutmaz.

~~~python
from mavflow import __version__

print(__version__)  # 2.0
~~~

<a id="first-heartbeat"></a>
### İlk heartbeat mesajını alma

Bu async örnek UDP 14550 portunu dinler ve EventFlow'un sınırlı
`AsyncEventQueue` kuyruğundan heartbeat bekler. MAVLink router tek transport
okuyucusu olarak kalır; async tüketici için ek bir okuma thread'i açılmaz.

~~~python
import asyncio

from eventflow import AsyncEventQueue
from mavflow import MavEndpoint, MavRuntime


async def main() -> None:
    runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
    async with AsyncEventQueue(
        runtime.messages,
        routing_key="HEARTBEAT",
        capacity=64,
        overflow="drop_oldest",
    ) as heartbeats:
        runtime.start()
        try:
            envelope = await asyncio.wait_for(heartbeats.receive(), timeout=10.0)
        except asyncio.TimeoutError:
            print("Süre dolmadan heartbeat alınamadı")
        else:
            print("İlk heartbeat:", envelope.message)
        finally:
            runtime.stop()
    runtime.close()


asyncio.run(main())
~~~

Senkron kodda `runtime.messages.wait_for(routing_key="HEARTBEAT")` kullanın;
sonuç bir `MavMessageEnvelope`, asıl pymavlink mesajı ise `.message` alanıdır.
Transport yaşam döngüsünü runtime yönetir.

<a id="run-an-example"></a>
### Bir örneği çalıştırma

Önce Mavflow'u kurun, ardından komutları depo kökünden çalıştırın. Telemetri
örneklerinin endpoint'e veri gönderen bir araç, simülatör veya MAVLink router'a
ihtiyacı vardır.

~~~bash
python -m examples.telemetry_async --host 0.0.0.0 --port 14550 --timeout 10
python -m examples.telemetry --host 0.0.0.0 --port 14550 --timeout 10
python -m examples.filtered_telemetry --system 1 --seconds 30
python -m examples.message_history --message-type HEARTBEAT --limit 20
python -m examples.application_packets
~~~

`telemetry_async` asyncio ile mesaj almayı, `telemetry` ise senkron runtime'ı
gösterir. `filtered_telemetry` tek bir kaynak system'den position ve attitude
mesajlarını seçer; `message_history` bir mesajı bekleyip router'ın tuttuğu
geçmişi ve sayaçları yazdırır. `application_packets` araç bağlantısı açmadan
paket kodlama ve birleştirmeyi gösterir.

<a id="choose-api-layer"></a>
### API katmanını seçme

| Bileşen | Kullanım amacı |
| --- | --- |
| `MavConnection` | `pymavlink` transportunu açmak ve ham MAVLink mesajlarını gönderip almak. |
| `MavMessageRouter` | Tek okuma döngüsünü sahiplenip tipli mesaj envelope'ları yayımlamak. |
| `MavClient` | Connection, router ve EventFlow isimli kanallarını birleştirmek. |
| `MavRuntime` | Client yaşam döngüsünü ve isteğe bağlı uygulama servislerini yönetmek. |
| EventFlow `EventBus` | Envelope'lara abone olmak, filtrelemek, beklemek ve geçmişi sorgulamak. |
| EventFlow `AsyncEventQueue` | Senkron bus'ı asyncio içinde sınırlı kuyrukla tüketmek. |

Normal uygulama giriş noktası olarak `MavRuntime` kullanın. EventFlow doğrudan
çalışma zamanı bağımlılığıdır; `EventBus`, `EventFilter`, `EventEngine` ve
`AsyncEventQueue` tiplerini `eventflow` paketinden import edin.

<a id="core-concepts"></a>
## Temel kavramlar

<a id="endpoints"></a>
### Endpoint ve bağlantılar

`MavEndpoint` değiştirilemez bir bağlantı tanımıdır. `tcp(host, port)`,
`udp(host, port)` ve `serial(device, baud=...)` yardımcıları yaygın endpoint
biçimlerini oluşturur:

~~~python
from mavflow import MavEndpoint

udp = MavEndpoint.udp("0.0.0.0", 14550)
tcp = MavEndpoint.tcp("192.0.2.10", 5760)
serial = MavEndpoint.serial("/dev/ttyUSB0", baud=57600)
~~~

Endpoint ayrıca yerel system ve component kimliklerini, MAVLink dialect'ini,
yeniden bağlanma ayarını ve heartbeat timeout değerini taşır. Varsayılan URI
`udp:127.0.0.1:14550`'dir. Araç, radyo, simülatör veya router tarafından
beklenen adresi ve transport biçimini seçin.

`MavConnection`, alttaki `pymavlink` bağlantısının sahibidir. `mavflow`
import edildiğinde bir cihaz açılmaz ve transport modülü yüklenmez; `pymavlink`
gerçek bağlantı oluşturulurken yüklenir. Connection; yaşam döngüsü metotlarını,
ham mesaj erişimini, gönderim yardımcılarını ve araç hedef bilgilerini sağlar.
Bu düşük seviyede `MavConnection.receive()`, pymavlink'in yerel mesaj tipi
filtresini (`str`, `list[str]` veya `set[str]`) ve condition ifadesini doğrudan
iletir. Router çalışırken tek okuyucu olarak kalmalıdır; yönlendirilmiş telemetri
için `runtime.messages` kanalını kullanın.

<a id="router"></a>
### Tek okuyucu ve mesaj router'ı

`MavMessageRouter` bağlantının okuma döngüsünü sahiplenir. Gelen mesajı bir
kez okur, `MavMessageEnvelope` içine alır ve EventFlow `EventBus` üzerinden bir
kez yayımlar. Bus abonelikleri `message_type` anahtarına göre indeksler;
anahtarlı abonelik ilgisiz MAVLink tiplerinin callback'lerini atlar ve kayıt
sırasını korur.

Tek okuyucu sahipliği transport okumasını ve yönlendirmeyi tek yerde tutar.
Router callback'leri senkron teslimat sırasında çalışır; kısa tutun veya pahalı
işleri sınırlı bir worker'a gönderin. `MavClient` ve `MavRuntime`, aynı native
EventFlow bus'ını `messages`, engine'i `events` olarak doğrudan sunar.

<a id="filtering"></a>
### Filtreleme ve mesaj geçmişi

EventFlow routing key ve predicate kullanarak abone olun. Routing key router'ın
indeksli bus'ında bir mesaj tipini seçer; ek kaynak kimlikleri ve uygulama
koşulları için `EventFilter` kullanın:

~~~python
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
source_one = EventFilter(predicate=lambda envelope: envelope.source_system == 1)
subscription = runtime.messages.subscribe(
    lambda envelope: print(envelope.message),
    routing_key="GLOBAL_POSITION_INT",
    event_filter=source_one,
)
runtime.start()
try:
    runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=10.0)
finally:
    subscription.cancel()
    runtime.close()
~~~

Envelope; `source_component`, `message_id`, sequence ve alım zamanını da taşır.
Native MAVLink condition koşulunu `runtime.connection.evaluate_condition(condition)`
çağıran bir EventFlow predicate'i içinde değerlendirebilirsiniz.

<a id="lifecycle"></a>
### Yaşam döngüsü ve bileşim

`MavRuntime` bir client'ı ve istendiğinde uygulama channel'ını, peer'i ve
dispatcher'ı bir araya getirir. `start()` transportu ve sahip olunan uygulama
servislerini bağımlılık sırasına göre başlatır. `stop()` ters sırayla kapatır
ve kapatma hatalarını toplar. İlk örnekteki gibi senkron context manager olarak
da kullanılabilir.

Ayrı bağlantılar için ayrı runtime'lar oluşturun. Daha alt seviyeli connection
ve router'ı paylaşması veya yapılandırması gereken uygulamalar mevcut bir
`MavClient` nesnesini runtime'a verebilir.

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

`runtime.messages` doğrudan EventFlow `EventBus` nesnesidir. `subscribe()` ve
`once()` native EventFlow abonelikleri döndürür; callback'e
`MavMessageEnvelope` verilir:

~~~python
from mavflow import MavEndpoint, MavRuntime

runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
subscription = runtime.messages.subscribe(
    lambda envelope: print(envelope.message),
    routing_key="HEARTBEAT",
)
runtime.start()
try:
    first = runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=10.0)
    print("İlk heartbeat:", first.message if first is not None else None)
finally:
    subscription.cancel()
    runtime.close()
~~~

Ek koşullar için `EventFilter` veya `event_type` ve `predicate` abonelik
argümanlarını kullanın. `routing_key` EventFlow tarafından indeks üzerinde
uygulanır; telemetri aboneliği ilgisiz mesaj tiplerini taramaz.

<a id="wait-and-history"></a>
### Mesaj bekleme ve geçmişi inceleme

Tek bir gelecek mesaj için EventFlow `wait_for()` kullanın. Sınırlı EventFlow
geçmişi bus üzerinden doğrudan erişilebilir ve aynı native filtreleri kabul eder:

~~~python
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

with MavRuntime(MavEndpoint.udp("0.0.0.0", 14550)) as runtime:
    envelope = runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=10.0)
    history = runtime.messages.history
    heartbeat_filter = EventFilter(predicate=lambda item: item.message_type == "HEARTBEAT")
    recent = history.query(heartbeat_filter, limit=20) if history is not None else ()
    latest = history.latest(heartbeat_filter) if history is not None else None
    router_state = runtime.state.router
~~~

`wait_for()` süre aşımında `None` döndürür. Geçmiş envelope'ları yayın sırasıyla
ve `history_limit` (varsayılan 512) sınırı içinde tutar; kalıcı bir telemetri
logu değildir. Router istatistikleri okuma/dispatch hatalarını, tahmini kaybı
ve teslimat kalitesini içerir.

<a id="async-applications"></a>
## Async uygulamalar

MAVLink transport okuması senkron router'ın sahipliğinde kalır. Aynı
envelope'ları asyncio içinde almak için EventFlow `AsyncEventQueue` kullanın.
Filtre kuyruğun kapasitesini tüketmeden uygulanır; tüketici yavaşladığında
`drop_oldest` en yeni telemetriyi korur.

~~~python
import asyncio

from eventflow import AsyncEventQueue
from mavflow import MavEndpoint, MavRuntime


async def main() -> None:
    runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
    async with AsyncEventQueue(
        runtime.messages,
        routing_key="HEARTBEAT",
        capacity=64,
        overflow="drop_oldest",
    ) as heartbeats:
        runtime.start()
        try:
            while True:
                envelope = await heartbeats.receive()
                print(envelope.message)
        finally:
            runtime.stop()
    runtime.close()


asyncio.run(main())
~~~

Kuyruk MAVLink reader'ı sahiplenmez ve her yayın için asyncio task'ı oluşturmaz.
`stats()` kuyruk derinliğini ve düşen event sayısını verir. Tüketici task'ını
normal şekilde iptal edin; `async with` EventFlow aboneliğini kaldırır.

<a id="application-packets"></a>
## Uygulama paketleri

İsteğe bağlı uygulama protokolü JSON uyumlu Mavflow paketlerini MAVLink
`V2_EXTENSION` mesajları üzerinden taşır. Bu protokol standart telemetri
yönlendirmesinden ayrıdır; yalnızca normal MAVLink mesajlarına ihtiyaç duyan
uygulamaların etkinleştirmesi gerekmez.

<a id="codec"></a>
### Paket kodlama ve birleştirme

`MavApplicationPacket` paket tipi, JSON uyumlu payload, paket ID'si,
zaman damgası, kaynak kimlikleri ve yanıt bayrağını taşır.
`MavApplicationCodec` paketi sınırlandırılmış parçalara böler, checksum
ekler ve decode sırasında parçaları doğrular. `MavApplicationAssembler`
eksik paketlerin sayısını, belleğini ve yaşam süresini sınırlandırarak
parçaları birleştirir.

Codec araç bağlantısı açılmadan da kullanılabilir:

~~~python
from mavflow import (
    MavApplicationAssembler,
    MavApplicationCodec,
    MavApplicationPacket,
)

packet = MavApplicationPacket(
    "vehicle.health",
    {"ready": True, "battery_percent": 84},
    packet_id=42,
)
parcalar = MavApplicationCodec.encode(packet)

assembler = MavApplicationAssembler()
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
from mavflow import MavEndpoint, MavRuntime


def saglik_bilgisi(_request: object) -> dict[str, object]:
    return {"ready": True, "battery_percent": 84}


endpoint = MavEndpoint.udp("0.0.0.0", 14550)
runtime = MavRuntime(endpoint, application_role="vehicle")
handler = runtime.handle("vehicle.health.query", saglik_bilgisi)

try:
    with runtime:
        # Uzak peer artık vehicle.health.query isteği gönderebilir.
        ...
finally:
    handler.cancel()
~~~

Handler bir `MavApplicationPacket` alır. Bir mapping,
`MavApplicationResult` veya `None` döndürebilir. İstek yanıt bekliyorsa
Mavflow handler sonucunu isteği gönderen peer'a iletir. Handler kaydı iptal
edilebilir bir abonelik döndürür.

Tek yönlü paket için `notify()`, ilişkili bir yanıt beklendiğinde `request()`
kullanın. İstek bir ID taşır, yanıt tipi filtresi kabul eder ve süre dolmadan
eşleşen yanıt gelmezse `TimeoutError` yükseltir:

~~~python
with MavRuntime(endpoint, application_role="ground-station") as link:
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

`MavRemoteLogRecord` ve `MavRemoteLogBatch` uygulama log kayıtlarını
tanımlar ve protokol payload'larına dönüştürülebilir. Bu tipler doğrulanmış
kayıt ve batch modelleri sağlar; Python logging handler'ı yapılandırmaz veya
logları yerel depolamaya yazmaz.

<a id="troubleshooting"></a>
## Sorun giderme

| Belirti | Kontroller |
| --- | --- |
| `wait_for()` `None` döndürüyor veya async receive süre aşımına uğruyor. | Göndericinin yapılandırılmış UDP/TCP endpoint'e veri gönderdiğini, araç veya simülatörün yayın yaptığını ve istenen mesaj tipinin mevcut olduğunu doğrulayın. Bağlantının açılması daha uzun sürüyorsa timeout'u artırın. |
| Async receive bekliyor veya kuyruk kapasitesi doluyor. | `AsyncEventQueue.stats()` ve yapılandırılan taşma politikasını kontrol edin. Yalnızca gereken mesaj tiplerini yönlendirin, olayları daha hızlı işleyin veya bellek uygunsa `capacity` değerini artırın. |
| Uygulama `request()` çağrısı süre aşımına uğruyor. | Uzak peer'ın çalıştığını, paket tipiyle yanıt tipinin kayıtlı handler ile eşleştiğini ve system/component ID'lerinin bu peer'a yönlendirdiğini kontrol edin. |

EventFlow `wait_for()` bekleme süresi dolunca `None` döndürür. Async queue
receive `asyncio.TimeoutError`, uygulama request'i ise `TimeoutError` yükseltir.
Bir görev birden fazla API kullanıyorsa bu durumları ayrı ele alın.

<a id="operational-boundaries"></a>
## Güvenlik ve çalışma sınırları

Mavflow MAVLink transportu ve protokol modellerini sağlar. Araç politikasını,
komut yetkilendirmesini, görev davranışını veya uygulamanın telemetriyi nasıl
saklayacağını belirlemez.

MAVLink history'si, async kuyruk kapasitesi, uygulama parçası birleştirme ve
handler işleri bellekte sınırlıdır. EventFlow mesaj geçmişini, abonelikleri,
queue taşma davranışını ve sınırlı worker yürütmesini yönetir. Mavflow yalnızca
tek reader, paket birleştirme, istek korelasyonu ve MAVLink kayıp tahmini gibi
protokole özel durumu tutar. `runtime.stop()` EventFlow kanallarını yeniden
başlatma için açık bırakır; `runtime.close()` EventFlow engine'ini de kapatır.

<a id="composition"></a>
## Yığını birleştirme

Temel telemetri yolu `MavEndpoint` → `MavConnection` →
`MavMessageRouter` → `MavClient` → `MavRuntime` şeklindedir.
Her katmanın tek bir işi vardır: adresi tanımlamak, transportu sahiplenmek,
okuma döngüsünü yönetmek, senkron bir fasad sağlamak ve uygulama yaşam
döngüsünü koordine etmek.

Uygulama rolü aynı client ve router üzerine paket channel'ı, peer durumu ve
sınırlı dispatcher ekler:

~~~python
from mavflow import MavClient, MavEndpoint, MavRuntime

endpoint = MavEndpoint.udp("0.0.0.0", 14550)
client = MavClient(endpoint)
runtime = MavRuntime(
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
| `MavEndpoint` | TCP, UDP veya seri bağlantı tanımını normalize etmek. |
| `MavConnection` | `pymavlink` transportu, yaşam döngüsünü ve ham gönderme/alma işlemlerini yönetmek. |
| `MavMessageRouter` | Tek okuma döngüsünü sahiplenip anahtarlı mesaj envelope'ları yayımlamak. |
| `MavClient` | Connection, router ve EventFlow engine'ini birleştirmek. |
| `MavRuntime` | Client ve isteğe bağlı uygulama servislerini birlikte başlatıp durdurmak. |
| `MavApplicationChannel` | Uygulama parçalarını MAVLink `V2_EXTENSION` üzerinden taşımak. |
| `MavApplicationPeer` | Bildirim ve ilişkili istekleri göndermek, peer canlılığını izlemek. |
| `MavApplicationDispatcher` | Kayıtlı paket handler'larını EventFlow sınırlı worker pool'u ile çalıştırmak. |

### Runtime ve EventFlow API'leri

| Erişim | Davranış |
| --- | --- |
| `runtime.messages` | Doğrudan EventFlow `EventBus[MavMessageEnvelope]`; `subscribe`, `once`, `wait_for` ve history sağlar. |
| `runtime.events` | MAVLink ve uygulama kanallarını sahiplenen doğrudan EventFlow `EventEngine`. |
| `runtime.start()` / `stop()` / `close()` | Başlatmak, yeniden başlatma için durdurmak veya durdurup EventFlow kanallarını kapatmak. |
| `runtime.notify(packet_type, payload=None)` | Tek yönlü uygulama paketi göndermek; application role gerekir. |
| `runtime.request(packet_type, payload=None, ...)` | İlişkili uygulama isteği göndermek ve yanıt beklemek. |
| `runtime.handle(packet_type, handler)` | Uygulama handler'ı kaydetmek; iptal edilebilir kayıt döndürmek. |

Bileşen olaylarını `mavlink.connection.errors`, `mavlink.router.errors`,
`mavlink.application.packets` ve `mavlink.dispatcher.errors` gibi EventFlow
kanallarından alın. Mavflow ikinci bir toplu hata bus'ı oluşturmaz.

<a id="public-types"></a>
### Public tipler ve hatalar

Mavflow transport, envelope, uygulama protokolü, peer, dispatcher ve uzak log
tiplerini `mavflow` paketinden dışa aktarır. EventFlow yapılarını doğrudan
çalışma zamanı bağımlılığı olan `flowevent` dağıtımının `eventflow` paketinden
import edin: `EventBus`, `EventFilter`, `EventEngine`, `AsyncEventQueue` ve
`BoundedWorkPool`.

| Alan | Mavflow public isimleri |
| --- | --- |
| Transport ve yönlendirme | `MavEndpoint`, `MavConnection`, `MavMessageRouter`, `MavMessageEnvelope`, `MavHeader`, `MavMessage`, `MavRouterStats` |
| Runtime | `MavClient`, `MavRuntime`, `MavRuntimeState` |
| Uygulama protokolü | `MavApplicationPacket`, `MavApplicationCodec`, `MavApplicationAssembler`, `MavApplicationChannel`, `MavApplicationPeer`, `MavApplicationDispatcher`, `MavApplicationHandlerRegistry`, `MavApplicationResult`, `MavApplicationResponse` |
| Uzak log | `MavRemoteLogLevel`, `MavRemoteLogRecord`, `MavRemoteLogBatch` |

`MavHeader` ve `MavMessage`, `mavflow.contracts` modülünde tanımlanır ve
`mavflow` paketinden de dışa aktarılır. Bileşen hata değerleri ayrı EventFlow
kanallarında kalır; `MavRouterError`, `MavUnavailableError` ve
`MavApplicationProtocolError` `mavflow` paketinden import edilebilir.

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

[version-shield]: https://img.shields.io/badge/version-v2.0-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
