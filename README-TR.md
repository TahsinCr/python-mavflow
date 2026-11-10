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
Geliştirilmekte olan 2.0 serisi yayımlanmış EventFlow 1.3.2 sürümünü
(`flowevent>=1.3.2,<1.4`) ister.

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


async def main(runtime: MavRuntime) -> None:
    async with AsyncEventQueue(
        runtime.messages,
        capacity=64,
        overflow="drop_oldest",
    ) as heartbeats:

        async def next_heartbeat():
            while True:
                envelope = await heartbeats.receive()
                if envelope.message_type == "HEARTBEAT":
                    return envelope

        try:
            envelope = await asyncio.wait_for(next_heartbeat(), timeout=10.0)
        except asyncio.TimeoutError:
            print("Süre dolmadan heartbeat alınamadı")
        else:
            print("İlk heartbeat:", envelope.message)


runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
try:
    runtime.start()
    asyncio.run(main(runtime))
finally:
    runtime.close()
~~~

`MavRuntime.start()` senkrondur ve heartbeat bekleyebilir. Bu örnek bağlantı
başlatmasını asyncio döngüsüne girmeden yaparak event loop'un bloklanmasını
önler. Runtime'ın tek router thread'i MAVLink reader olarak kalır; async
tüketiciler doğrudan EventFlow kuyruğunu kullanır.

Senkron kodda `runtime.messages.wait_for()` metoduna EventFlow
`EventFilter` verin. Sonuç bir `MavMessageEnvelope` olur; asıl pymavlink mesajı
`.message` alanındadır. Transport yaşam döngüsünü runtime yönetir.

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
Yerleşik `pymavlink` TCP ve UDP transport'ları IPv6 literal adreslerini
desteklemez; Mavflow transport'un açamayacağı bir endpoint üretmek yerine IPv6
host'larını erken reddeder.

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
kez yayımlar. EventFlow 1.3.2 envelope alanına göre indeksli yönlendirme
sağlamaz; mesaj tipi seçimi native `EventFilter` predicate'iyle yapılır. Her
predicate her yayınlanan envelope için çalışır; Mavflow ikinci bir yönlendirme
indeksi tutmaz.

Tek okuyucu sahipliği transport okumasını ve yönlendirmeyi tek yerde tutar.
Router callback'leri senkron teslimat sırasında çalışır; kısa tutun veya pahalı
işleri sınırlı bir worker'a gönderin. `MavClient` ve `MavRuntime`, aynı native
EventFlow bus'ını `messages`, engine'i `events` olarak doğrudan sunar.

<a id="filtering"></a>
### Filtreleme ve mesaj geçmişi

Mesaj tipini ve kaynak kimliklerini EventFlow'un native `EventFilter`
predicate'iyle seçin:

~~~python
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
position_filter = EventFilter(
    predicate=lambda envelope: (
        envelope.message_type == "GLOBAL_POSITION_INT" and envelope.source_system == 1
    )
)
subscription = runtime.messages.subscribe(
    lambda envelope: print(envelope.message),
    event_filter=position_filter,
)
runtime.start()
try:
    runtime.messages.wait_for(
        event_filter=EventFilter(predicate=lambda envelope: envelope.message_type == "HEARTBEAT"),
        timeout=10.0,
    )
finally:
    subscription.cancel()
    runtime.close()
~~~

Envelope; `source_component`, `message_id`, sequence ve alım zamanını da taşır.
Native MAVLink condition koşulunu `runtime.connection.evaluate_condition(condition)`
çağıran bir EventFlow predicate'i içinde değerlendirebilirsiniz. Bu çağrı
pymavlink'in Python ifade değerlendiricisini kullanır; yalnızca güvenilir
condition metinlerini verin. Güvenilmeyen değerler için normal bir EventFlow
predicate'i kullanın.

<a id="lifecycle"></a>
### Yaşam döngüsü ve bileşim

`MavRuntime` bir client'ı ve istendiğinde uygulama channel'ını, peer'i ve
dispatcher'ı bir araya getirir. `start()` transportu ve sahip olunan uygulama
servislerini bağımlılık sırasına göre başlatır. `stop()` ters sırayla kapatır
ve kapatma hatalarını toplar. Eşzamanlı `start()`, `stop()`, `reconnect()` ve
`close()` çağrıları tam yaşam döngüsü geçişi boyunca sıraya alınır. Senkron
context manager olarak da kullanılabilir.

Ayrı bağlantılar için ayrı runtime'lar oluşturun. Daha alt seviyeli connection
ve router'ı paylaşması veya yapılandırması gereken uygulamalar mevcut bir
`MavClient` nesnesini runtime'a verebilir.

<a id="event-delivery"></a>
### Event teslimatı

Mavflow event bus ve abonelik handle'ları için `flowevent` kullanır. Runtime
mesaj, paket ve hata event'leri bu kütüphanenin sözleşmelerini doğrudan
kullanır; Mavflow ikinci bir event bus uygulamaz.

EventFlow senkron aboneleri event'i yayınlayan thread üzerinde çalıştırır.
MAVLink mesajlarında bu, router'ın tek reader thread'idir; callback'leri kısa
ve bloklamayan şekilde tutun. Ayrı tüketici gerektiren işler için
`AsyncEventQueue` veya uygulama dispatcher'ını kullanın. Bekleyen bir
`peer.request()` çağrısını mesaj callback'i içinden yapmayın; yanıtı almak için
router'ın okumaya devam etmesi gerekir.

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
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
subscription = runtime.messages.subscribe(
    lambda envelope: print(envelope.message),
    event_filter=EventFilter(predicate=lambda item: item.message_type == "HEARTBEAT"),
)
runtime.start()
try:
    first = runtime.messages.wait_for(
        event_filter=EventFilter(predicate=lambda item: item.message_type == "HEARTBEAT"),
        timeout=10.0,
    )
    print("İlk heartbeat:", first.message if first is not None else None)
finally:
    subscription.cancel()
    runtime.close()
~~~

Mesaj tipi, kaynak ve diğer koşullar için native `EventFilter` API'sini kullanın.
Mavflow mesajlarının hepsi aynı `MavMessageEnvelope` Python tipini kullandığı
için EventFlow 1.3.2 her envelope için predicate'i değerlendirir. Filtreleri
küçük tutun ve beklenen abone sayısında callback fan-out ölçümü yapın.

<a id="wait-and-history"></a>
### Mesaj bekleme ve geçmişi inceleme

Tek bir gelecek mesaj için EventFlow `wait_for()` kullanın. Sınırlı EventFlow
geçmişi bus üzerinden doğrudan erişilebilir ve aynı native filtreleri kabul eder:

~~~python
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

with MavRuntime(MavEndpoint.udp("0.0.0.0", 14550)) as runtime:
    heartbeat_filter = EventFilter(predicate=lambda item: item.message_type == "HEARTBEAT")
    envelope = runtime.messages.wait_for(
        event_filter=heartbeat_filter,
        timeout=10.0,
    )
    history = runtime.messages.history
    recent = history.query(heartbeat_filter, limit=20) if history is not None else ()
    latest = history.latest(heartbeat_filter) if history is not None else None
    router_state = runtime.state.router
~~~

`wait_for()` süre aşımında `None` döndürür. Geçmiş envelope'ları yayın sırasıyla
ve `history_limit` (varsayılan 512) sınırı içinde tutar; kalıcı bir telemetri
logu değildir. Router istatistikleri okuma/dispatch hatalarını, tahmini kaybı
ve teslimat kalitesini içerir.
Modulo 256 sequence farkında 64'e kadarki boşluklar sayılır. 65 ile 191
arasındaki ileri sıçrama, kesin kayıp sayısı iddia etmeden sequence başlangıcını
yeniler; 64'e kadarki geriye sıralanma yok sayılır. Bu sayaçlar tahmindir, link
seviyesinde teslimat garantisi değildir.

<a id="async-applications"></a>
## Async uygulamalar

MAVLink transport okuması senkron router'ın sahipliğinde kalır. Aynı
envelope'ları asyncio içinde almak için EventFlow `AsyncEventQueue` kullanın.
EventFlow 1.3.2 kuyruğa eklemeden önce predicate uygulayamadığından tüm mesaj
tipleri sınırlı kapasiteyi paylaşır. Örnekteki gibi `receive()` sonrasında
filtreleyin; ilgisiz mesajlar da taşma politikasını tetikleyebilir. `drop_oldest`
kullanılırken ilgisiz telemetri, tüketici almadan önce heartbeat'i kuyruktan
çıkarabilir. EventFlow 1.3.2 queue predicate'i veya mesaj önceliği sağlamaz;
Mavflow bu davranışı yeniden yazmak yerine EventFlow kuyruğunu doğrudan kullanır.

~~~python
import asyncio

from eventflow import AsyncEventQueue
from mavflow import MavEndpoint, MavRuntime


async def main(runtime: MavRuntime) -> None:
    async with AsyncEventQueue(
        runtime.messages,
        capacity=64,
        overflow="drop_oldest",
    ) as heartbeats:
        while True:
            envelope = await heartbeats.receive()
            if envelope.message_type == "HEARTBEAT":
                print(envelope.message)


runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
try:
    runtime.start()
    asyncio.run(main(runtime))
finally:
    runtime.close()
~~~

Bağlantı heartbeat bekleyebilecekse senkron `MavRuntime.start()` çağrısını
asyncio'ya girmeden yapın. Tek MAVLink reader router tarafından yönetilir.

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
parçaları birleştirir. Ayrıca tek MAVLink kaynağının paylaşılan bütçenin tümünü
tüketmemesi için kaynak başına eksik paket ve byte sınırı uygular.

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
kullanın. Yanıtın istek ID'si, paket tipi, kaynak system'i ve component'i
doğrulanır. Süre dolmadan eşleşen yanıt gelmezse `TimeoutError` yükselir:

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
Request deadline'ı senkron gönderim ve yanıt bekleme süresinin tamamını kapsar.
Bloklanan transport gönderimini Python kesemez; çağrı gönderim tamamlandıktan
sonra deadline'ın dolduğunu bildirir.

Mavflow handler'a vermeden önce her paketin kaynağını peer'ın güncel system ve
component kimliğiyle doğrular; dinamik hedefler de bu kontrole dahildir.
Callable hedef o anda `0` döndürürse kimlik bilinmiyor kabul edilir ve paket
reddedilir; doğrudan verilen `0`, varsa channel kaynak allowlist'leriyle
sınırlandırılmış wildcard anlamını korur. Birden fazla system veya component'ten
paket kabul etmek istiyorsanız channel kaynak allowlist'lerini kullanın.

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
| Async receive bekliyor veya kuyruk kapasitesi doluyor. | `AsyncEventQueue.stats()` ve yapılandırılan taşma politikasını kontrol edin. EventFlow 1.3.2 tüm envelope tiplerini kuyruğa alır; alımdan sonra filtreleyin, olayları daha hızlı işleyin veya bellek uygunsa `capacity` değerini artırın. |
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
başlatma için açık bırakır; `runtime.close()` bileşen temizliği başarıyla
tamamlandığında EventFlow engine'ini de kapatır. Temizlik başarısız olursa
engine açık kalır ve `close()` yeniden denenebilir.

`MavConnection.is_connected`, Mavflow'un bir pymavlink bağlantı nesnesi
tuttuğunu belirtir; bağlantının sağlıklı olduğunu göstermez. Sağlık gözlemi için
peer canlılığı ve mesaj zamanlarını kullanın. MAVLink system/component ID'leri
ve uygulama protokolünün CRC32 checksum'ı göndericiyi doğrulamaz. Güvenilmeyen
bağlantılarda imzalama ve izin verilen kaynak ID'lerini bağlantıyı başlatmadan
önce yapılandırın:

~~~python
import os

from mavflow import MavEndpoint, MavRuntime

signing_key = bytes.fromhex(os.environ["MAVLINK_SIGNING_KEY"])
endpoint = MavEndpoint.udp(
    "0.0.0.0",
    14550,
    expected_system=1,
    expected_component=1,
    signing_key=signing_key,
)
runtime = MavRuntime(
    endpoint,
    application_role="gcs",
    channel_options={"source_systems": (1,), "source_components": (1,)},
)
runtime.start()
~~~

`signing_key`, 32 byte'lık MAVLink 2 gizli anahtarı olmalıdır. Mavflow, ilk
heartbeat beklenmeden önce pymavlink imzalamasını kurar, giden frame'leri
imzalar ve imzasız mesaj kabulünü kapalı tutar. pymavlink import edilmeden önce
MAVLink 2 dialect'i seçmek için `MAVLINK20=1` ayarlayın; etkin transport MAVLink
1 bildirirse Mavflow signing anahtarını reddeder. İsteğe bağlı beklenen system ve
component ID'leri heartbeat keşfini filtreler; varsayılan application channel
kaynak izin listesine ve peer hedeflerine aktarılır. Tamsayı `peer_options`
hedefleri de bu izin listesini oluşturur. Açık `channel_options` kaynak
filtreleri önceliklidir, böylece bilinçli çoklu kaynak politikası korunur.
Anahtar verilmezse pymavlink'in varsayılan imzasız davranışı geçerli olur. Peer
hedefindeki `0`, wildcard anlamına gelir; system/component ID'leri kriptografik
kimlik değildir.

`MavConnection.evaluate_condition()` condition metnini pymavlink'in Python
ifade değerlendiricisine verir. Bu metoda güvenilmeyen metin göndermeyin.
Çalışan senkron handler'lar Python tarafından zorla kesilemez; I/O ve döngüler
kendi sonlu zaman sınırlarına sahip olmalıdır. `MavApplicationDispatcher.stop()`
çalışan handler'ların dönmesini bekler.

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
python -m coverage report --fail-under=85
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
python -m benchmarks.benchmark_router --subscribers 10 --matching-ratio 10 --output router-run.json
python -m benchmarks.benchmark_codec --output codec-run.json
python -m pyperf show --metadata --stats router-run.json
python -m pyperf compare_to --table benchmarks/results/router.json router-run.json
~~~

Her çalıştırma için kullanılmamış bir çıktı dosyası seçin; pyperf mevcut
dosyanın üzerine yazmaz. Router benchmark'ı önceden doldurulmuş bellek içi
connection üzerinden tek okuyuculu teslimatı ölçer. Eşleşen EventFlow callback
fan-out sayısını karşılaştırmak için `--subscribers` (ör. 1, 10 veya 100), eşleşen
trafik oranı için `--matching-ratio` (0, 10, 50 veya 100) değerini değiştirin.
Tek bir tamamlama gözlemcisi, ölçümün son envelope dispatch'ini kapsamasını sağlar
ve metadata'da belirtilir. Bu router teslimat benchmark'ıdır; gerçek UDP loopback
testi ayrı bir doğruluk kontrolüdür. Codec benchmark'ı
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
