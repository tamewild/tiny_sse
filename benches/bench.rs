use criterion::{criterion_group, criterion_main, Criterion};
use futures::{StreamExt, TryStreamExt};
use std::convert::Infallible;
use std::sync::LazyLock;
use tokio::runtime::Builder;

macro_rules! iterate_on_stream {
    ($chunks:ident, $ty:ty) => {
        async {
            let stream = <$ty>::new(
                futures::stream::iter(&*$chunks).map(Ok::<_, Infallible>)
            );

            stream.try_for_each(|event| async {
                criterion::black_box(event);
                Ok(())
            }).await.unwrap();
        }
    };
}

fn sse(c: &mut Criterion) {
    static REGULAR_CHUNKS: LazyLock<Vec<String>> = LazyLock::new(|| {
        serde_json::from_str(
            std::fs::read_to_string("misc/regular_chunks.json").unwrap().as_str()
        ).unwrap()
    });
    static IRREGULAR_CHUNKS: LazyLock<Vec<String>> = LazyLock::new(|| {
        serde_json::from_str(
            std::fs::read_to_string("misc/irregular_chunks.json").unwrap().as_str()
        ).unwrap()
    });

    LazyLock::force(&REGULAR_CHUNKS);
    LazyLock::force(&IRREGULAR_CHUNKS);

    let rt = Builder::new_current_thread().build().unwrap();

    let mut regular = c.benchmark_group("regular");

    regular.bench_function("tiny_sse", |b| {
        b.to_async(&rt).iter(|| iterate_on_stream!(REGULAR_CHUNKS, tiny_sse::EventStream<_>))
    });

    regular.bench_function("eventsource-stream", |b| {
        b.to_async(&rt).iter(|| iterate_on_stream!(REGULAR_CHUNKS, eventsource_stream::EventStream<_>))
    });

    regular.finish();

    let mut irregular = c.benchmark_group("irregular");

    irregular.bench_function("tiny_sse", |b| {
        b.to_async(&rt).iter(|| iterate_on_stream!(IRREGULAR_CHUNKS, tiny_sse::EventStream<_>));
    });

    irregular.bench_function("eventsource-stream", |b| {
        b.to_async(&rt).iter(|| iterate_on_stream!(IRREGULAR_CHUNKS, eventsource_stream::EventStream<_>));
    });

    irregular.finish();
}

criterion_group!(benches, sse);
criterion_main!(benches);