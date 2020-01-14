//
// test for neural net classes
//

// system headers
#include <assert.h>
#include <string>

// local headers
#include "BaseNN.H"

int
main(int argc, char **argv)
{
    assert(argc == 2);
    string topology_file_path(argv[1]);

    NNTopology topology;
    topology.load_from_file(topology_file_path);

    cout << "Topology: " << endl;
    cout << topology << endl;

    cout << "Neural Net (1): " << endl;
    NeuralNet nn(topology);

    cout << "Neural Net (2): " << endl;
    cout << nn << endl;

    NNVector inv, outv;
    for (int i=0; i<nn.layer(0).number_of_neurons(); ++i)
    {
        inv.push_back(1.0);
    }
    cout << "In-Vector: " << inv << endl;

    nn.apply(inv, outv);
    cout << "Out-Vector: " << outv << endl;

    return 0;
}
