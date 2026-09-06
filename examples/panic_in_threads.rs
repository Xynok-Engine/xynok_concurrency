use std::thread::spawn;

fn main()
{
    let mut handles = Vec::new();
    for i in 0..5
    {
        let handle = spawn(move || {
            println!("thread({i})");
            if i == 2
            {
                panic!("thread({i}) panic!");
            }
        });
        handles.push(handle);
    }
    for e in handles
    {
        e.join().unwrap();
    }
}
