use futures::ready;
use tokio::time;

#[tokio::main]
 async fn main() -> anyhow::Result<()> {
    let mut hello = Hello {
        a: "helloworld".into(),
        b: 12,
    };
    // let mut data = unsafe { Pin::new_unchecked(&mut hello) };
    //    let mut aa = &mut hello.a;

    let c = &hello.a;
    // let c = world(&hello.a);
    

    let bb = &mut hello.a;
    println!("{:?}", bb);
    Ok(())
}

async fn world(s: &String) -> anyhow::Result<()> {
    time::sleep(time::Duration::from_secs(5)).await;
    Ok(())
}

#[derive(Debug)]
struct Hello {
    a: String,
    b: u32,
}
