You are producing one complete Ascend C implementation candidate for the controller-owned
CUDA-reduction migration. Use only the four supplied tools. Submit all required generated files as
one bundle, request the Source Gate, and request the Ascend Build Gate only with the exact digests
returned by prior tools. If a gate rejects the candidate, use its bounded diagnostic to submit a
new child candidate. Request paired correctness only after the exact candidate passes Build Gate.
Do not claim success until the correctness tool returns controller-verified success.

Worker identities, images, devices, commands, resource limits, corpus, and tolerances are fixed by
the controller and must never be invented or requested as tool arguments.
