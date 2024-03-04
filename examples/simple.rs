use fn_graph::{Edge, FnGraphBuilder, WouldCycle};
use resman::{IntoFnRes, Resources};

fn main() -> Result<(), WouldCycle<Edge>> {
    let mut resources = Resources::new();
    resources.insert(0u32);

    let fn_graph = {
        let mut fn_graph_builder = FnGraphBuilder::new();
        let [double, cube, add_five] = fn_graph_builder.add_fns([
            double.into_fn_res(),
            cube.into_fn_res(),
            add_five.into_fn_res(),
        ]);
        fn_graph_builder.add_logic_edges([(add_five, double), (double, cube)])?;
        fn_graph_builder.build()
    };

    fn_graph
        .iter()
        .map(|f| f.call(&resources))
        .for_each(|event| {
            println!(
                "{:8}: {:>4} -> {:>4}",
                event.fn_name, event.n_before, event.n_after
            )
        });

    Ok(())
}

fn add_five(n: &mut u32) -> Event {
    let n_before = *n;
    let n_after = n_before + 5;
    *n = n_after;

    Event {
        fn_name: "add_five",
        n_before,
        n_after,
    }
}

fn double(n: &mut u32) -> Event {
    let n_before = *n;
    let n_after = n_before * 2;
    *n = n_after;

    Event {
        fn_name: "double",
        n_before,
        n_after,
    }
}

fn cube(n: &mut u32) -> Event {
    let n_before = *n;
    let n_after = n_before * n_before * n_before;
    *n = n_after;

    Event {
        fn_name: "cube",
        n_before,
        n_after,
    }
}

#[derive(Debug)]
struct Event {
    fn_name: &'static str,
    n_before: u32,
    n_after: u32,
}
