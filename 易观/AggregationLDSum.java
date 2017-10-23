package aggregation;

// import com.facebook.presto.operator.aggregation.state.SliceState;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import state.SliceState;

/*
计算漏斗的聚合函数, 步骤二
 */
@AggregationFunction("ld_sum")
public class AggregationLDSum extends AggregationBase {

    @InputFunction
    public static void input(SliceState state,
                             @SqlType(StandardTypes.INTEGER) long xwho_state,       // 每个用户的状态
                             @SqlType(StandardTypes.INTEGER) long events_count) {   // 查询事件的个数
        // 获取state状态
        Slice slice = state.getSlice();

        // 初始化state, 长度为events_length个int
        if (null == slice) {
            slice = Slices.allocate((int) events_count * 4);
        }

        // 计算用户数
        for (int status = 0; status < xwho_state; ++status) {
            int index = status * 4;
            slice.setInt(index, slice.getInt(index) + 1);
        }

        // 返回状态
        state.setSlice(slice);
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState) {
        // 获取状态
        Slice slice = state.getSlice();
        Slice otherslice = otherState.getSlice();

        // 更新状态并返回结果
        if (null == slice) {
            state.setSlice(otherslice);
        } else {
            for (int index = 0; index < slice.length(); index += 4) {
                slice.setInt(index, slice.getInt(index) + otherslice.getInt(index));
            }
            state.setSlice(slice);
        }
    }

    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();

        // 数据为空, 返回一个空数组
        if (null == slice) {
            BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), 0);
            out.writeObject(blockBuilder.build());
            out.closeEntry();
            return;
        }

        // 构造结果: [A:100, B:50, C:10, ......]
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), slice.length() / 4);
        for (int index = 0; index < slice.length(); index += 4) {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getInt(index));
        }

        // 返回结果
        out.writeObject(blockBuilder.build());
        out.closeEntry();
    }

}
