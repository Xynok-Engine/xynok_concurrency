struct RingBuffer
{
    data:        [i32; 4],
    write_index: usize,
    len:         usize,
}

impl RingBuffer
{
    fn new() -> Self
    {
        Self {
            data:        [0; 4],
            write_index: 0,
            len:         0,
        }
    }

    fn push(&mut self, value: i32)
    {
        self.data[self.write_index] = value;

        self.write_index = (self.write_index + 1) % self.data.len();

        self.len = (self.len + 1).min(self.data.len());
    }

    fn print(&self)
    {
        let start = if self.len == self.data.len() { self.write_index } else { 0 };

        for i in 0..self.len
        {
            let index = (start + i) % self.data.len();
            print!("{} ", self.data[index]);
        }

        println!();
    }
}

fn main()
{
    let mut buffer = RingBuffer::new();

    buffer.push(10);
    println!("buffer elements: {:?}", buffer.data);
    buffer.push(20);
    println!("buffer elements: {:?}", buffer.data);
    buffer.push(30);
    buffer.push(40);
    println!("buffer elements: {:?}", buffer.data);
    buffer.print(); // 10 20 30 40

    buffer.push(50);
    buffer.print(); // 20 30 40 50
    println!("buffer elements: {:?}", buffer.data);

    buffer.push(60);
    buffer.print(); // 30 40 50 60
}
